package no.ruter.sb.grunnplattform.commons.kafka.async.consumer.config

import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumer
import org.slf4j.LoggerFactory
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component
import java.lang.invoke.MethodHandles

@Component
@ConditionalOnProperty("kafka.async.consumer.health.enabled", havingValue = "true", matchIfMissing = true)
class AsyncConsumerHealthIndicator(private val asyncConsumer: AsyncConsumer) : HealthIndicator {

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    override fun health(): Health {
        return if (asyncConsumer.isHealthy()) {
            Health.up().build()
        } else {
            logger.warn("Async-consumer is unhealthy!")
            Health.down().build()
        }
    }
}