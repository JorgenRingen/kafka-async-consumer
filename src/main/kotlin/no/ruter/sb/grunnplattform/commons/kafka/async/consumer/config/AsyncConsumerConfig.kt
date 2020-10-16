package no.ruter.sb.grunnplattform.commons.kafka.async.consumer.config

import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import io.micrometer.core.instrument.internal.TimedExecutorService
import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumer
import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumerGracefulShutdown
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.lang.invoke.MethodHandles
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

/**
 * Expects the application to provide a bean of type AsyncConsumerRegistration to instantiate the AsyncConsumer
 */
@ConditionalOnClass(AsyncConsumerRegistration::class)
@Configuration
class AsyncConsumerConfig(private val registration: AsyncConsumerRegistration) {

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    init {
        logger.debug("AsyncConsumerConfig initiated with registration: $registration")
    }

    /**
     * Thread-pool which is responsible of running "tasks" (processing of ConsumerRecords in parallel).
     * Should use an unbounded work-queue and "back-pressure" in the kafka-consumer.
     */
    @Bean(destroyMethod = "")
    fun parallelExecutor(): ExecutorService =
        Executors.newFixedThreadPool(registration.threads, NamedThreadFactory("async-consumer-parallel"))

    /**
     * Wraps and monitors the the async-consumer-main thread-pool
     */
    @Bean(destroyMethod = "")
    fun monitoredParallelExecutor() =
        TimedExecutorService(
            registration.meterRegistry,
            parallelExecutor(),
            "async.consumer.parallel-executor",
            "",
            emptyList()
        )

    /**
     * Provide metrics for the async-consumer processor pool
     */
    @Bean
    fun parallelExecutorMetrics() =
        ExecutorServiceMetrics(parallelExecutor(), "async.consumer.parallel-executor", emptyList())

    /**
     * Single-threaded pool which is responsible of running AsyncConsumer in a separate thread.
     */
    @Bean(destroyMethod = "")
    fun mainExecutor(): ExecutorService =
        Executors.newSingleThreadExecutor(NamedThreadFactory("async-consumer-main-executor", appendThreadNum = false))

    @Bean(destroyMethod = "")
    fun asyncConsumer(
        monitoredParallelExecutor: ExecutorService,
        parallelExecutor: ExecutorService
    ) = AsyncConsumer(
        consumer = registration.consumer,
        topics = registration.topics,
        executorService = monitoredParallelExecutor,
        workQueue = (parallelExecutor as ThreadPoolExecutor).queue,
        bufferSize = registration.bufferSize,
        fullBufferWaitMillis = registration.fullBufferWaitMillis,
        taskSupplier = registration.taskSupplier
    )

    @Bean(destroyMethod = "")
    fun asyncConsumerGracefulShutdown(
        asyncConsumer: AsyncConsumer,
        parallelExecutor: ExecutorService
    ) = AsyncConsumerGracefulShutdown(asyncConsumer, parallelExecutor)

    @Bean
    fun asyncConsumerLifecycleHandler(
        asyncConsumer: AsyncConsumer,
        asyncConsumerGracefulShutdown: AsyncConsumerGracefulShutdown,
        mainExecutor: ExecutorService
    ): AsyncConsumerLifecycleHandler =
        AsyncConsumerLifecycleHandler(asyncConsumer, asyncConsumerGracefulShutdown, mainExecutor)

}

