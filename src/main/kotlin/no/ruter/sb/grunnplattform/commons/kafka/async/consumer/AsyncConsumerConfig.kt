package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import io.micrometer.core.instrument.internal.TimedExecutorService
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

/**
 * Expects the application to provide:
 * - all "kafka.async.consumer.*"-properties and to provide
 * - a bean of type KafkaConsumer that's named "asyncKafkaConsumer"
 * - a bean of type AsyncConsumerTaskSupplier that returns Runnables which will process consumer-records
 */
@ConditionalOnProperty("kafka.async.consumer.enabled", havingValue = "true", matchIfMissing = false)
@ConditionalOnClass(KafkaConsumer::class)
@Configuration
class AsyncConsumerConfig(
    @Value("\${kafka.async.consumer.threads}") private val threads: Int,
    @Value("\${kafka.async.consumer.maxWorkQueueSize}") private val maxWorkQueueSize: Int,
    @Value("\${kafka.async.consumer.fullWorkQueueSleepMillis}") private val fullWorkQueueSleepMillis: Long,
    @Value("\${kafka.async.consumer.topics}") private val topics: List<String>,
    @Qualifier("asyncKafkaConsumer") private val asyncKafkaConsumer: KafkaConsumer<*, *>,
    private val asyncConsumerTaskSupplier: AsyncConsumerTaskSupplier
) {

    @Bean(destroyMethod = "")
    fun asyncConsumerExecutor(): ExecutorService = Executors.newFixedThreadPool(threads)

    @Bean(destroyMethod = "")
    fun monitoredAsyncConsumerExecutor(meterRegistry: MeterRegistry, asyncConsumerExecutor: ExecutorService) =
        TimedExecutorService(meterRegistry, asyncConsumerExecutor, "asyncConsumerExecutor", "", emptyList())

    @Bean(destroyMethod = "")
    fun asyncConsumerListenerExecutor(): ExecutorService = Executors.newSingleThreadExecutor()

    @Bean
    fun executorServiceMetrics(asyncConsumerExecutor: ExecutorService) =
        ExecutorServiceMetrics(asyncConsumerExecutor(), "asyncConsumerExecutor", emptyList())

    @Bean(destroyMethod = "")
    fun asyncConsumer(
        monitoredAsyncConsumerExecutor: ExecutorService,
        asyncConsumerExecutor: ExecutorService
    ) = AsyncConsumer(
        consumer = asyncKafkaConsumer,
        topics = topics,
        executorService = monitoredAsyncConsumerExecutor,
        workQueue = (asyncConsumerExecutor as ThreadPoolExecutor).queue,
        maxWorkQueueSize = maxWorkQueueSize,
        fullWorkQueueWaitMillis = fullWorkQueueSleepMillis,
        taskSupplier = asyncConsumerTaskSupplier
    )

    @Bean(destroyMethod = "")
    fun asyncConsumerShutdown(
        asyncConsumer: AsyncConsumer,
        asyncConsumerExecutor: ExecutorService
    ) = AsyncConsumerShutdown(asyncConsumer, asyncConsumerExecutor)

    @Bean
    fun asyncConsumerLifecycleHandler(
        asyncConsumer: AsyncConsumer,
        asyncConsumerShutdown: AsyncConsumerShutdown,
        asyncConsumerListenerExecutor: ExecutorService
    ): AsyncConsumerLifecycleHandler =
        AsyncConsumerLifecycleHandler(asyncConsumer, asyncConsumerShutdown, asyncConsumerListenerExecutor)

}

