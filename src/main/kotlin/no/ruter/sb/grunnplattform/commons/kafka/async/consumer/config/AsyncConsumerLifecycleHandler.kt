package no.ruter.sb.grunnplattform.commons.kafka.async.consumer.config

import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumer
import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumerGracefulShutdown
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.context.event.EventListener
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.PreDestroy

/**
 * Starts and stops the async-consumer
 */
class AsyncConsumerLifecycleHandler(
    private val asyncConsumer: AsyncConsumer,
    private val asyncConsumerGracefulShutdown: AsyncConsumerGracefulShutdown,
    private val mainExecutor: ExecutorService
) {

    companion object {
        val started = AtomicBoolean(false)
    }

    @EventListener
    fun applicationStarted(applicationStartedEvent: ApplicationStartedEvent) {
        mainExecutor.submit(asyncConsumer)
        started.set(true)
    }

    @PreDestroy
    fun preDestroy() {
        if (started.get()) {
            asyncConsumerGracefulShutdown.shutdown()
            mainExecutor.shutdown()
        }
    }
}