package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Application should implement this interface so that the AsyncConsumer
 * can retrieve AsyncConsumerTasks which is responsible of processing
 * ConsumerRecords
 */
interface AsyncConsumerTaskSupplier {
    fun getTask(consumerRecord: ConsumerRecord<*, *>): AsyncConsumerTask?
}