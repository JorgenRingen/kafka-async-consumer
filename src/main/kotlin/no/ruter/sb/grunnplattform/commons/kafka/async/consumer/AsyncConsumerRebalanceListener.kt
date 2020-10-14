package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles

/**
 * Keeps track of assigned partitions so we can pause during consumption
 */
class AsyncConsumerRebalanceListener : ConsumerRebalanceListener {

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private val assignedPartitions = mutableSetOf<TopicPartition>()

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
        logger.debug("Partitions assigned: $partitions")
        assignedPartitions.addAll(partitions.toSet())
    }

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
        logger.debug("Partitions revoked: $partitions")
        assignedPartitions.removeAll(partitions)
    }

    fun getAssignedPartitions(): Set<TopicPartition> {
        return assignedPartitions.toSet()
    }
}