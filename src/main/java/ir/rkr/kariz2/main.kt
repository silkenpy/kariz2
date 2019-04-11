package ir.rkr.kariz2


import com.typesafe.config.ConfigFactory
import ir.rkr.kariz2.kafka.KafkaConnector
import ir.rkr.kariz2.netty.NettyServer
import ir.rkr.kariz2.redis.RedisConnector
import ir.rkr.kariz2.rest.JettyRestServer
import ir.rkr.kariz2.util.KarizMetrics
import mu.KotlinLogging
import java.net.InetAddress


const val version = 0.1

/**
 * Kariz main entry point.
 */


fun main(args: Array<String>) {

    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val karizMetrics = KarizMetrics()

    val kafkaGroup = "${InetAddress.getLocalHost().hostName}${System.currentTimeMillis()}"
    val kafka = KafkaConnector(config.getString("kafka.topic"), config, karizMetrics,kafkaGroup)
    val redis = RedisConnector( config, karizMetrics)

    NettyServer(kafka, redis, config, karizMetrics)
    JettyRestServer(redis, config,karizMetrics)

    logger.info { "Kariz V$version is ready :D" }


}