package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

class AsyncConsumerGracefulShutdown(
    private val asyncConsumer: AsyncConsumer,
    private val parallelExecutor: ExecutorService,
    private val closeApplicationResources: () -> Unit,
) {

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    /**
     * Shutdowns all resources gracefully.
     * 1. stop the consumer to avoid fetching more records from kafka
     * 2. wait for fetched records to be submitted for processing
     * 3. wait for records submitted for processing to be completed
     * 4. close any application-resources used by AsyncConsumerTasks
     */
    fun shutdown() {

        logger.info("Shutting down AsyncConsumer...")

        asyncConsumer.stop()

        waitForAsyncConsumerToSubmitAllConsumedRecords()

        shutdownParallelExecutorGracefully()

        logger.debug("Closing application resources")
        closeApplicationResources()

        logger.info("Shutdown of AsyncConsumerProcessor completed!")
    }

    private fun waitForAsyncConsumerToSubmitAllConsumedRecords() {
        while (!asyncConsumer.submittedAllRecords()) {
            logger.debug("Waiting for AsyncConsumerProcessor to submit remaining records...")
            Thread.sleep(1000)
        }
    }

    private fun shutdownParallelExecutorGracefully() {
        kotlin.runCatching {
            logger.debug("Shutting down processing thread-pool...")
            parallelExecutor.shutdown()

            while (!parallelExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.info("Awaiting termination... Tasks remaining: ${asyncConsumer.bufferedCount()}")
            }

        }.onFailure {
            logger.error("Error while awaiting termination, enforcing shutdown!", it)
            forceShutdown()
            Thread.currentThread().interrupt()
        }
    }

    private fun forceShutdown() {
        val rejectedTasks = parallelExecutor.shutdownNow()
        if (rejectedTasks.size > 0) {
            logger.error("${rejectedTasks.size} rejected tasks after shutdown! This might lead to unprocessed messages!")
            rejectedTasks.forEach {
                logger.warn("Rejected task: $it")
            }
        }
    }
}