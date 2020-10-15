package no.ruter.sb.grunnplattform.commons.kafka.async.consumer.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import io.micrometer.core.instrument.internal.TimedExecutorService
import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumer
import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumerGracefulShutdown
import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.AsyncConsumerTaskSupplier
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.lang.invoke.MethodHandles
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

/**
 * Expects the application to provide:
 * - all "kafka.async.consumer.*"-properties declared in constructor
 * - a bean of type KafkaConsumer that's named "asyncKafkaConsumer"
 * - a bean of type AsyncConsumerTaskSupplier that returns "Runnables" which will process consumer-records
 */
@ConditionalOnClass(KafkaConsumer::class)
@Configuration
class AsyncConsumerConfig(
    @Value("\${kafka.async.consumer.threads}") private val threads: Int,
    @Value("\${kafka.async.consumer.bufferSize}") private val bufferSize: Int,
    @Value("\${kafka.async.consumer.fullBufferWaitMillis}") private val fullBufferWaitMillis: Long,
    @Value("\${kafka.async.consumer.topics}") private val topics: List<String>,
    @Qualifier("asyncKafkaConsumer") private val asyncKafkaConsumer: Consumer<*, *>,
    private val asyncConsumerTaskSupplier: AsyncConsumerTaskSupplier
) {

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    init {
        logger.debug("AsyncConsumerConfig initiated: [threads=$threads bufferSize=$bufferSize, fullBufferWaitMillis=$fullBufferWaitMillis, topics=$topics]")
    }


    /**
     * Thread-pool which is responsible of running "tasks" (processing of ConsumerRecords in parallel).
     * Should use an unbounded work-queue and "back-pressure" in the kafka-consumer.
     */
    @Bean(destroyMethod = "")
    fun parallelExecutor(): ExecutorService =
        Executors.newFixedThreadPool(threads, NamedThreadFactory("async-consumer-parallel"))

    /**
     * Wraps and monitors the the async-consumer-main thread-pool
     */
    @Bean(destroyMethod = "")
    fun monitoredParallelExecutor(meterRegistry: MeterRegistry) =
        TimedExecutorService(meterRegistry, parallelExecutor(), "async-consumer-parallel-executor", "", emptyList())

    /**
     * Provide metrics for the async-consumer processor pool
     */
    @Bean
    fun parallelExecutorMetrics() =
        ExecutorServiceMetrics(parallelExecutor(), "async-consumer-parallel-executor", emptyList())

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
        consumer = asyncKafkaConsumer,
        topics = topics,
        executorService = monitoredParallelExecutor,
        workQueue = (parallelExecutor as ThreadPoolExecutor).queue,
        bufferSize = bufferSize,
        fullBufferWaitMillis = fullBufferWaitMillis,
        taskSupplier = asyncConsumerTaskSupplier
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

