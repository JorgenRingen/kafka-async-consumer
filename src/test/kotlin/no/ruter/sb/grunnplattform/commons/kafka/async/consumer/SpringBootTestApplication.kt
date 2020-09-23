package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import java.util.*

////////////////////////////////////////////
// Spring-boot application used when testing
@SpringBootApplication
class TestApplication {

    @Bean(destroyMethod = "")
    fun asyncKafkaConsumer() = KafkaConsumer<String, String>(Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:$EMBEDDED_KAFKA_PORT")
        put(ConsumerConfig.GROUP_ID_CONFIG, "async-test")
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer().javaClass)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer().javaClass)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true)
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50)
    })

    @Bean
    fun taskSupplier() = taskSupplierMock

    @Bean
    fun meterRegistry(): MeterRegistry = SimpleMeterRegistry()
}