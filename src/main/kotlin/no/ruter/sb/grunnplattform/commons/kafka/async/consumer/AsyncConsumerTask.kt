package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles

/**
 * Should be implemented and returned by `AsyncConsumerTaskSupplier`.
 * The implementation handles the actual processing of consumer-records.
 *
 * NB: AsyncConsumer only handles "at-most-once" (i.e. no retries of failed records)
 */
abstract class AsyncConsumerTask(val consumerRecord: ConsumerRecord<*, *>) : Runnable {

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    override fun run() {
        kotlin.runCatching {
            doRun()
        }.onFailure {
            logger.error("Exception while processing record - Should be handled by the implementation. ConsumerRecord: $consumerRecord.", it)
        }
    }

    abstract fun doRun()
}