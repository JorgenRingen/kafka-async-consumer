package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.context.event.EventListener
import java.util.concurrent.ExecutorService
import javax.annotation.PreDestroy

/**
 * Starts and stops the async-consumer
 */
class AsyncConsumerLifecycleHandler(
    private val asyncConsumer: AsyncConsumer,
    private val asyncConsumerShutdown: AsyncConsumerShutdown,
    private val asyncConsumerListenerExecutor: ExecutorService
) {

    @EventListener
    fun applicationStarted(applicationStartedEvent: ApplicationStartedEvent) {
        asyncConsumerListenerExecutor.submit(asyncConsumer)
    }

    @PreDestroy
    fun preDestroy() {
        asyncConsumerShutdown.shutdown()
        asyncConsumerListenerExecutor.shutdown()
    }
}