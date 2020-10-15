package no.ruter.sb.grunnplattform.commons.kafka.async.consumer

import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.waitAtMost
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import java.time.Duration
import java.util.*

// needs to be open on machine where test is running (todo: fix more stable solution)
const val EMBEDDED_KAFKA_PORT = 9098

val taskSupplierMock = mockk<AsyncConsumerTaskSupplier>()

@SpringBootTest(
    classes = [TestApplication::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "kafka.async.consumer.threads=2",
        "kafka.async.consumer.bufferSize=10",
        "kafka.async.consumer.fullBufferWaitMillis=100",
        "kafka.async.consumer.topics=foo,bar"
    ]
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

