package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.ruter.sb.grunnplattform.commons.kafka.async.consumer.config.AsyncConsumerRegistration
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.waitAtMost
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.kafka.test.context.EmbeddedKafka
import java.time.Duration
import java.util.*

// needs to be open on machine where test is running (todo: fix more stable solution)
const val EMBEDDED_KAFKA_PORT = 9098

val taskSupplierMock = mockk<AsyncConsumerTaskSupplier>()

@SpringBootTest(
    classes = [TestApplication::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@EmbeddedKafka(ports = [EMBEDDED_KAFKA_PORT], controlledShutdown = true)
class AsyncConsumerTest {

    // used synchronized-list as it's shared by tasks
    private val processedRecords = Collections.synchronizedList<ConsumerRecord<*, *>>(arrayListOf())

    @BeforeEach
    fun beforeEach() {
        class TestableAsyncConsumerTask(consumerRecord: ConsumerRecord<*, *>) : AsyncConsumerTask(consumerRecord) {
            override fun process(consumerRecord: ConsumerRecord<*, *>) {
                processedRecords.add(consumerRecord)
                Thread.sleep(10) // simulate processing taking time...
            }
        }

        every { taskSupplierMock.getTask(any()) } answers {
            TestableAsyncConsumerTask(it.invocation.args[0] as ConsumerRecord<*, *>)
        }

    }

    @AfterEach
    fun afterEach() {
        clearMocks(taskSupplierMock)
    }

    @Test
    fun `should submit consumed records to AsyncConsumerTask`() {
        val numberOfRecords = 200
        val kafkaProducer = kafkaProducer()

        // we produce to 2 topics, so divide by 2
        for (i in 1..(numberOfRecords / 2)) {
            kafkaProducer.send(ProducerRecord("foo", UUID.randomUUID().toString(), "testValue"))
            kafkaProducer.send(ProducerRecord("bar", UUID.randomUUID().toString(), "testValue"))
        }

        // wait until all tasks has been processed
        waitAtMost(Duration.ofSeconds(20)).untilAsserted {
            verify(exactly = numberOfRecords) { taskSupplierMock.getTask(any()) }
            assertEquals(processedRecords.size, numberOfRecords)
        }

        // should be 200 distinct keys
        val recordKeys = processedRecords.map { it.key() as String }
        assertThat(recordKeys.distinct()).hasSize(numberOfRecords)

        // should be 100 records from foo and 100 from bar
        val topics = processedRecords.map { it.topic() }
        assertThat(topics.filter { it == "foo" }).hasSize(100)
        assertThat(topics.filter { it == "bar" }).hasSize(100)
    }

    fun kafkaProducer() = KafkaProducer<String, String>(Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:$EMBEDDED_KAFKA_PORT")
        put(ProducerConfig.CLIENT_ID_CONFIG, "async-test-producer")
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer().javaClass)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer().javaClass)
        put(ProducerConfig.ACKS_CONFIG, "1")
    })
}

@SpringBootApplication
class TestApplication {
    @Bean
    fun asyncConsumerRegistration(): AsyncConsumerRegistration {
        return AsyncConsumerRegistration(
            threads = 2,
            bufferSize = 10,
            fullBufferWaitMillis = 100,
            topics = listOf("foo", "bar"),
            consumer = asyncKafkaConsumer(),
            taskSupplier = taskSupplierMock,
            meterRegistry = SimpleMeterRegistry()
        )
    }

    private fun asyncKafkaConsumer() = KafkaConsumer<String, String>(Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:$EMBEDDED_KAFKA_PORT")
        put(ConsumerConfig.GROUP_ID_CONFIG, "async-test")
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer().javaClass)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer().javaClass)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true)
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10)
    })
}

