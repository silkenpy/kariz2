package ir.rkr.kariz2.kafka


import com.typesafe.config.Config
import ir.rkr.kariz2.util.KarizMetrics
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.net.InetAddress
import java.time.Duration
import java.util.*
import kotlin.collections.HashMap


class KafkaConnector(val topicName: String, config: Config, val karizMetrics: KarizMetrics,val group:String) {

    val consumer: KafkaConsumer<ByteArray, ByteArray>
    val producer: KafkaProducer<ByteArray, ByteArray>
    private val logger = KotlinLogging.logger {}

    init {

        val hostName = InetAddress.getLocalHost().hostName

        val producerCfg = Properties()
        producerCfg.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        producerCfg.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        config.getObject("kafka.producer").forEach({ x, y -> println("$x --> $y"); producerCfg.put(x, y.unwrapped()) })
        producer = KafkaProducer(producerCfg)

        val consumerCfg = Properties()

        config.getObject("kafka.consumer").forEach({ x, y -> println("kafka config $x --> $y"); consumerCfg.put(x, y.unwrapped()) })
        consumerCfg.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        consumerCfg.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        consumerCfg.put("group.id", "${group}")
        consumerCfg.put("client.id", "${group}${System.currentTimeMillis()}")
        consumerCfg.put("auto.offset.reset", "earliest")
        consumerCfg.put("enable.auto.commit", "false")
        consumer = KafkaConsumer(consumerCfg)

        Thread.sleep(100)
    }

    fun get(): HashMap<String, String> {

//        karizMetrics.MarkKafkaGetCall(1)

        val msg = HashMap<String, String>()

        return try {

            consumer.subscribe(Collections.singletonList(topicName))
            val res = consumer.poll(Duration.ofMillis(700))
            karizMetrics.MarkKafkaGetRecords(res.count().toLong())
            res.records(topicName).forEach { it -> msg[String(it.key())] = String(it.value()) }
            karizMetrics.MarkKafkaGetDuplicate(res.count().toLong()- msg.size.toLong())
            msg

        } catch (e: java.lang.Exception) {
            karizMetrics.MarkKafkaGetFail(1)
            msg
        }
    }


    fun put(key: String, value: String): Boolean {

        karizMetrics.MarkKafkaPutCall(1)

        try {
            val res = producer.send(ProducerRecord(topicName, key.toByteArray(), value.toByteArray()), object : Callback {
                override fun onCompletion(p0: RecordMetadata?, p1: Exception?) {

                    if (p1 != null) {

                        logger.error { "key=$key value=$value" }
                    }
                }
            })

            if (res.isDone) {
                karizMetrics.MarkKafkaPutFail(1)
                return false
            }

            karizMetrics.MarkKafkaPutRecords(1)
            return true

        } catch (e: Exception) {
            karizMetrics.MarkKafkaPutFail(1)
            logger.error { "Error in try catch" }
            return false
        }
    }

    fun commit() {
        consumer.commitAsync()
    }

}