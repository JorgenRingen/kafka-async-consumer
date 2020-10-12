package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
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
 * - `max.poll.interval.ms`: KafkaConsumer - Waiting for the work-queue to drain must not exceed this limit.
 * - `max.poll.records`: KafkaConsumer - How many records will be submitted to the work-queue for each poll.
 * - `maxWorkQueueSize`: AsyncConsumer - How big can the work-queue grow? Unbounded growth might lead to resource drain
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
 * @param maxWorkQueueSize Max number of tasks in the work-queue. Will stop polling if work-queue size exceeds this number.
 * @param fullWorkQueueWaitMillis How long to wait between each time work-queue limit is checked
 * @param taskSupplier Provides Runnables for processing consumer-records
 */
class AsyncConsumer(
    private val consumer: Consumer<*, *>,
    private val topics: List<String>,
    private val executorService: ExecutorService,
    private val workQueue: BlockingQueue<Runnable>,
    private val maxWorkQueueSize: Int,
    private val fullWorkQueueWaitMillis: Long,
    private val taskSupplier: AsyncConsumerTaskSupplier
) : Runnable {

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private val stopped = AtomicBoolean(false)
    private val submittedAllRecords = AtomicBoolean(false)

    private val consumerRebalanceListener = AsyncConsumerRebalanceListener()

    fun stop() {
        this.stopped.set(true)
        this.consumer.wakeup()
    }

    fun submittedAllRecords() = this.submittedAllRecords.get()

    override fun run() {
        try {
            subscribeToTopics()

            while (!stopped.get()) {
                val consumerRecords = pollRecords()
                submitRecords(consumerRecords)

                commitOffsets()

                if (stopped.get()) {
                    this.submittedAllRecords.set(true)
                    break
                }

                checkWorkQueueSize()
            }
        } catch (we: WakeupException) {
            if (!stopped.get()) {
                logger.error("Received WakeupException, but was not stopped!", we)
                throw we
            }
        } catch (e: Exception) {
            logger.error("Error while running AsyncConsumerProcessor", e)
        } finally {
            try {
                logger.debug("Closing kafka-consumer")
                consumer.close()
            } catch (e: Exception) {
                logger.error("Error while closing kafka-consumer", e)
            }
        }
    }

    private fun pollRecords(): ConsumerRecords<out Any, out Any> {
        val consumerRecords = consumer.poll(Duration.ofMillis(Long.MAX_VALUE))
        logger.debug("Polled ${consumerRecords.count()} consumer-records")
        return consumerRecords
    }

    private fun submitRecords(consumerRecords: ConsumerRecords<out Any, out Any>) {
        consumerRecords.forEach {
            val task = taskSupplier.getTask(it)
            executorService.submit(task)
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
     * Checks the work-queue size. If it's above the max-size we wait.
     * This is done so we don't overload the work-queue with new records.
     * Pauses the kafka-consumer while waiting.
     */
    private fun checkWorkQueueSize() {
        val waitingStarted = System.currentTimeMillis()
        val initialWorkQueueSize = workQueue.size
        while (workQueue.size > maxWorkQueueSize && !stopped.get()) {
            Thread.sleep(fullWorkQueueWaitMillis)
            consumer.pause(consumerRebalanceListener.getAssignedPartitions())
            logger.trace("Polling paused due to workQueueSize=${workQueue.size} >= maxQueueSize=${maxWorkQueueSize}")
        }

        val pausedPartitions = consumer.paused()
        if (pausedPartitions.isNotEmpty()) {
            consumer.resume(pausedPartitions)
            logger.info("Polling was paused for ${System.currentTimeMillis() - waitingStarted} milliseconds due to workQueueSize=$initialWorkQueueSize >= maxWorkQueueSize=$maxWorkQueueSize")
        }
    }

    private fun subscribeToTopics() {
        consumer.subscribe(topics, this.consumerRebalanceListener)
    }

    /**
     * Keeps track of assigned partitions so we can pause during consumption
     */
    inner class AsyncConsumerRebalanceListener : ConsumerRebalanceListener {

        private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private val assignedPartitions = mutableSetOf<TopicPartition>()

        override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
            logger.debug("Partitions assigned: $partitions")
            assignedPartitions.addAll(partitions.toSet())
        }

        override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
            logger.debug("Partitions revoked: $partitions")
            assignedPartitions.removeAll(assignedPartitions)
        }

        fun getAssignedPartitions(): Set<TopicPartition> {
            return assignedPartitions.toSet()
        }
    }
}