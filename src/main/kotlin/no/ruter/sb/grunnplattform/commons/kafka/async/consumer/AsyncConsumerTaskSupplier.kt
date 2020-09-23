package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Application should implement this interface so that the AsyncConsumer
 * can retrieve tasks (runnables) which will process consumer-records
 * async in a separate thread.
 */
interface AsyncConsumerTaskSupplier {
    fun getTask(consumerRecord: ConsumerRecord<*, *>): AsyncConsumerTask
}