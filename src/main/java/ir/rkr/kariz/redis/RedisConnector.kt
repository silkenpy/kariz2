package ir.rkr.kariz.redis

import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.kariz.kafka.KafkaConnector
import ir.rkr.kariz.netty.Command
import ir.rkr.kariz.util.KarizMetrics
import ir.rkr.kariz.util.randomItem
import mu.KotlinLogging
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool
import java.net.InetAddress
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.collections.ArrayList
import kotlin.concurrent.thread


class RedisConnector(config: Config, val karizMetrics: KarizMetrics) {


    private val redisPoolList = mutableListOf<JedisPool>()
    private val kafkaList = mutableListOf<KafkaConnector>()
    private val logger = KotlinLogging.logger {}
    private val gson = GsonBuilder().disableHtmlEscaping().create()


    init {

        val jedisCfg = GenericObjectPoolConfig<String>().apply {
            maxIdle = 20
            maxTotal = 100
            minIdle = 5
            maxWaitMillis = 90
        }

        val host = config.getString("redis.host")
        val port = config.getInt("redis.port")
        var password: String? = null

        if (config.hasPath("password")) password = config.getString("redis.password")
        val database = config.getInt("redis.database")

        redisPoolList.add(JedisPool(jedisCfg, host, port, 80, password, database))

        val hostName = InetAddress.getLocalHost().hostName
        val group = "${hostName}${System.currentTimeMillis()}"

        for (i in 1..5){
            kafkaList.add(KafkaConnector(config.getString("kafka.topic"), config, karizMetrics,group))
            Thread.sleep(10)
        }

        for (i in 1..5)
            Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({

                val kafka = kafkaList.get(i)

                val commands = kafka.get()

                if (commands.size > 0) {
                    val setCommands = ArrayList<String>()
                    commands.forEach { t, u ->

                        val parsed = gson.fromJson(u, Command::class.java)
                        when (parsed.cmd) {

                            "set" -> {
                                setCommands.add(parsed.key)
                                setCommands.add(parsed.value!!)
                            }

                            "del" -> del(parsed.key)

                            "expire" -> expire(parsed.key, parsed.ttl)

                            else -> {
                                logger.error { parsed.toString() }
                            }
                        }
                    }

                    mset(setCommands)
                    kafka.commit()
                }
            }, 0, 50, TimeUnit.MILLISECONDS)


    }


    fun get(key: String): Optional<String> {
        val pool = redisPoolList.randomItem().get()

        try {

            pool.resource.use { redis ->
                if (redis.isConnected) {
                    val value = redis.get(key)
                    if (value != null) return Optional.of(value)
                }
                return Optional.empty()
            }

        } catch (e: Exception) {
            logger.trace(e) { "There is no resource in pool for redis.get." }
            return Optional.empty()
        }
    }


    fun mget(keys: List<String>): MutableMap<String, Optional<String>> {

        val result = keys.map { it to Optional.empty<String>() }.toMap().toMutableMap()

        return try {

            val pool = redisPoolList.randomItem().get()
            pool.resource.use { redis ->

                keys.zip(redis.mget(*keys.toTypedArray())).toMap().forEach { k, v -> if (v != null) result[k] = Optional.of(v) }

                result
            }

        } catch (e: Exception) {
            logger.trace(e) { "There is no resource in pool for redis.mget." }
            keys.map { it to Optional.empty<String>() }.toMap().toMutableMap()
        }
    }

    fun mset(keyValues: List<String>): Boolean {

        val pool = redisPoolList.randomItem().get()
        return try {
            pool.resource.use { redis ->
                redis.mset(*keyValues.toTypedArray())
                true
            }
        } catch (e: Exception) {
            false
        }
    }

    fun set(key: String, value: String): Boolean {

        val pool = redisPoolList.randomItem().get()
        return try {
            val res = pool.resource.set(key, value)
            res == "OK"

        } catch (e: Exception) {
            logger.error { "redis set $e" }
            false
        }
    }


    fun del(key: String): Boolean {
        val pool = redisPoolList.randomItem().get()

        return try {
            pool.resource.del(key)
            true

        } catch (e: Exception) {
            logger.error { "redis del $e" }
            false
        }
    }

    fun expire(key: String, ttl: Long?): Boolean {

        val pool = redisPoolList.randomItem().get()
        return try {
            if (ttl != null) {
                pool.resource.expire(key, ttl.toInt())
                true
            } else
                false

        } catch (e: Exception) {
            logger.error { "redis del $e" }
            false
        }
    }


}