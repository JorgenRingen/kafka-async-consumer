package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class AsyncConsumerShutdown(
    private val asyncConsumer: AsyncConsumer,
    private val asyncConsumerExecutor: ExecutorService
) {

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    /**
     * Shutdowns all resources gracefully.
     * 1. stop the consumer to avoid fetching more records from kafka
     * 2. wait for fetched records to be submitted for processing
     * 3. wait for records submitted for processing to be completed
     */
    fun shutdown() {

        logger.info("Shutting down AsyncConsumerProcessor...")

        asyncConsumer.stop()

        waitForAsyncConsumerToSubmitAllConsumedRecords()

        shutdownAsyncConsumerExecutorGracefully()

        logger.info("Shutdown of AsyncConsumerProcessor completed!")
    }

    private fun waitForAsyncConsumerToSubmitAllConsumedRecords() {
        while (asyncConsumer.submittedAllRecords()) {
            logger.debug("Waiting for AsyncConsumerProcessor to submit remaining records...")
            Thread.sleep(2500)
        }
    }

    private fun shutdownAsyncConsumerExecutorGracefully() {
        kotlin.runCatching {
            logger.debug("Shutting down executor-service...")
            asyncConsumerExecutor.shutdown()

            while (!asyncConsumerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.debug("Awaiting termination... Tasks remaining: ${(asyncConsumerExecutor as ThreadPoolExecutor).queue.size}")
            }

        }.onFailure {
            logger.error("Error while awaiting termination, enforcing shutdown!", it)
            forceShutdown()
            Thread.currentThread().interrupt()
        }
    }

    private fun forceShutdown() {
        val rejectedTasks = asyncConsumerExecutor.shutdownNow()
        if (rejectedTasks.size > 0) {
            logger.warn("${rejectedTasks.size} rejected tasks after shutdown! This might lead to unprocessed messages!")
            rejectedTasks.forEach {
                logger.warn("Rejected task: $it")
            }
        }
    }
}