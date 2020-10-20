package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.time.Duration
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Polls kafka and delegates processing of records to separate async threads.
 * Instances of this class should be run in a separate thread and typically
 * one instance will be run per application-instance consuming from one or
 * more topics.
 *
 * The consumer waits in the poll-loop if "workQueueSize > workQueueSizeLimit" to avoid overloading the
 * executorService with tasks. Assigned partitions are paused while waiting.
 *
 * The following parameters needs to be tuned with care for each use-case:
 * - `max.poll.records`: KafkaConsumer - How many records will be submitted to the work-queue for each poll.
 * - `bufferSize`: AsyncConsumer - How big can the work-queue grow? Unbounded growth might lead to resource drain
 * and long latency. Scaling number of services running AsyncConsumer might be preferable to a big maxWorkQueueSize.
 *
 * NB!
 * Use carefully, this implementation only handles "at-most-once" semantics and no ordering guarantees.
 * Should be used where scaling is needed beyond kafka-partitions and where the "performance-characteristics" of a
 * thread-pool is preferred over standard kafka "partition-based in-order processing".
 * Message-drops might occur in some failure-situations.
 *
 * @param consumer The kafka-consumer used for polling
 * @param topics The topics to poll
 * @param executorService The ExecutorService which handles processing in a thread-pool
 *      Thread-pool should support an unbounded queue to avoid message-drops and rejection of tasks.
 * @param workQueue The work-queue backing the ExecutorService
 * @param bufferSize Max number of tasks in the work-queue. Will stop polling if work-queue size exceeds this number.
 * @param fullBufferWaitMillis How long to wait between each time work-queue limit is checked
 * @param taskSupplier Provides Runnables for processing consumer-records
 */
class AsyncConsumer(
    private val consumer: Consumer<*, *>,
    private val topics: List<String>,
    private val executorService: ExecutorService,
    private val workQueue: BlockingQueue<Runnable>,
    private val bufferSize: Int,
    private val fullBufferWaitMillis: Long,
    private val taskSupplier: AsyncConsumerTaskSupplier
) : Runnable {

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private val stopped = AtomicBoolean(false)
    private val healthy = AtomicBoolean(true)
    private val submittedAllRecords = AtomicBoolean(false)

    private val consumerRebalanceListener = AsyncConsumerRebalanceListener()

    fun stop() {
        this.stopped.set(true)
        this.consumer.wakeup()
    }

    override fun run() {
        try {
            subscribeToTopics()

            while (!stopped.get()) {
                val consumerRecords = pollRecords()

                submitForProcessing(consumerRecords)

                commitOffsets()

                if (stopped.get()) {
                    this.submittedAllRecords.set(true)
                    break
                }

                waitForBufferToDrain()
            }
        } catch (we: WakeupException) { // thrown if a long-poll was aborted when stopping
            if (!stopped.get()) {
                logger.error("Received WakeupException, but was not stopped!", we)
                throw we
            }
            this.submittedAllRecords.set(true)
        } catch (e: Exception) { // on errors in the "poll-loop" (should not happen)
            logger.error("Error while running AsyncConsumerProcessor", e)
            this.healthy.set(false)
        } finally { // cleanup
            try {
                logger.debug("Closing kafka-consumer")
                consumer.close()
            } catch (e: Exception) {
                logger.error("Error while closing kafka-consumer", e)
            }
        }
    }

    private fun pollRecords(): ConsumerRecords<out Any, out Any> {
        val consumerRecords = try {
            consumer.poll(Duration.ofMillis(Long.MAX_VALUE))
        } catch (e: SerializationException) {
            logger.error("SerializationException while consuming records - Records are skipped!", e)
            return ConsumerRecords.EMPTY
        }

        logger.debug("Polled ${consumerRecords.count()} consumer-records")
        return consumerRecords
    }

    private fun submitForProcessing(consumerRecords: ConsumerRecords<out Any, out Any>) {
        consumerRecords.forEach { consumerRecord->
            taskSupplier.getTask(consumerRecord)?.let { task ->
                executorService.submit(task)
            }
        }
        logger.debug("Submitted ${consumerRecords.count()} consumer-records for processing")
    }

    /**
     * Commit sync if stopped (meaning no more records will be polled),
     * otherwise commit async.
     */
    private fun commitOffsets() {
        if (stopped.get()) {
            consumer.commitSync()
        } else {
            consumer.commitAsync()
        }
    }

    /**
     * Checks the number of buffered tasks. If it's above the max-size we wait.
     * This is done so we don't overload the work-queue with new records (backpressure)
     * Pauses the kafka-consumer while waiting.
     */
    private fun waitForBufferToDrain() {
        val initialBufferCount = bufferedCount()
        val waitingStarted = System.currentTimeMillis()

        while (bufferIsOverloaded() && !stopped.get()) {
            consumer.pause(consumerRebalanceListener.getAssignedPartitions())
            logger.debug("Polling paused due to workQueueSize=${workQueue.size} >= maxQueueSize=${bufferSize}")
            runCatching { Thread.sleep(fullBufferWaitMillis) }
                .onFailure { logger.warn("Caught exception while waiting for buffer to drain", it) }
        }

        val pausedPartitions = consumer.paused()
        if (pausedPartitions.isNotEmpty()) {
            consumer.resume(pausedPartitions)
            logger.debug("Polling was paused for ${System.currentTimeMillis() - waitingStarted} milliseconds due to workQueueSize=$initialBufferCount >= maxWorkQueueSize=$bufferSize")
        }
    }

    private fun subscribeToTopics() {
        consumer.subscribe(topics, this.consumerRebalanceListener)
    }

    private fun bufferIsOverloaded() = bufferedCount() > bufferSize

    fun bufferedCount() = workQueue.size

    fun submittedAllRecords(): Boolean = submittedAllRecords.get()

    fun isHealthy() = this.healthy.get()

}