package no.ruter.sb.grunnplattform.commons.kafka.async.consumer.config

import io.micrometer.core.instrument.MeterRegistry
import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumerTaskSupplier
import org.apache.kafka.clients.consumer.Consumer

data class AsyncConsumerRegistration(
    val threads: Int,
    val bufferSize: Int = 500,
    val fullBufferWaitMillis: Long = 200,
    val topics: List<String>,
    val consumer: Consumer<*, *>,
    val taskSupplier: AsyncConsumerTaskSupplier,
    val meterRegistry: MeterRegistry,
) {

    override fun toString(): String {
        return "AsyncConsumerRegistration(threads=$threads, bufferSize=$bufferSize, fullBufferWaitMillis=$fullBufferWaitMillis, topics=$topics)"
    }
}