package no.ruter.sb.grunnplattform.commons.kafka.async.consumer.config

import io.micrometer.core.instrument.MeterRegistry
import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumerTaskSupplier
import org.apache.kafka.clients.consumer.Consumer

/**
 * @param threads Level of parallelism
 * @param bufferSize Size of buffer before pausing consumption
 * @param fullBufferWaitMillis Waiting between check of full buffer
 * @param topics Topics to consume from
 * @param consumer The kafka-consumer used to poll records
 * @param taskSupplier Returns "tasks" that will be executed per ConsumerRecord in parallel
 * @param meterRegistry Used for monitoring (mandatory because we want to "enforce" monitoring)
 * @param closeResources Resources used in async processing (KafkaProducer for example) will be closed gracefully after all tasks has finished.
 */
data class AsyncConsumerRegistration(
    val threads: Int,
    val bufferSize: Int = 500,
    val fullBufferWaitMillis: Long = 200,
    val topics: List<String>,
    val consumer: Consumer<*, *>,
    val taskSupplier: AsyncConsumerTaskSupplier,
    val meterRegistry: MeterRegistry,
    val closeResources: () -> Unit,
) {

    override fun toString(): String {
        return "AsyncConsumerRegistration(threads=$threads, bufferSize=$bufferSize, fullBufferWaitMillis=$fullBufferWaitMillis, topics=$topics)"
    }
}