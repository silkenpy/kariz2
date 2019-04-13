package ir.rkr.kariz2.redis

import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.kariz2.kafka.KafkaConnector
import ir.rkr.kariz2.netty.Command
import ir.rkr.kariz2.util.KarizMetrics
import ir.rkr.kariz2.util.randomItem
import mu.KotlinLogging
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.collections.ArrayList


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
            maxWaitMillis = 1000
        }

        val host = config.getString("redis.host")
        val port = config.getInt("redis.port")
        val feederNum = config.getInt("redis.feederNum") - 1
        val redisTimeout = config.getInt("redis.timeout")

        var password: String? = null

        if (config.hasPath("password")) password = config.getString("redis.password")
        val database = config.getInt("redis.database")

        for (i in 0..feederNum)
            redisPoolList.add(JedisPool(jedisCfg, host, port, redisTimeout, password, database))

        for (i in 0..feederNum) {
            println("kkakakakakakakakak $i")
            kafkaList.add(KafkaConnector(config.getString("kafka.topic"), config, karizMetrics, "$i"))
            Thread.sleep(10)
        }

        for (i in 0..feederNum)
            Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({

                val kafka = kafkaList.get(i)

                val commands = kafka.get()

                if (commands.size > 0) {
                    val setCommands = ArrayList<String>()
                    commands.forEach { _, u ->

                        val parsed = gson.fromJson(u, Command::class.java);
                        while (
                                !when (parsed.cmd) {

                                    "set" -> {
                                        setCommands.add(parsed.key)
                                        setCommands.add(parsed.value!!)
                                        true
                                    }

                                    "del" -> {
                                        del(parsed.key)

                                    }

                                    "expire" -> expire(parsed.key, parsed.ttl)

                                    else -> {
                                        logger.error { parsed.toString() }
                                        true
                                    }
                                }) {
                            Thread.sleep(500)
                        }
                    }

                    while (!mset(setCommands)) {
                        Thread.sleep(500)
                    }
                    kafka.commit()
                }
            }, 0, 100, TimeUnit.MILLISECONDS)
    }


    fun get(key: String): Optional<String> {

        karizMetrics.MarkRedisGetCall(1)
        val pool = redisPoolList.randomItem().get()

        try {

            pool.resource.use { redis ->
                if (redis.isConnected) {
                    val value = redis.get(key)
                    if (value != null) {
                        karizMetrics.MarkRedisGetAvailable(1)
                        return Optional.of(value)
                    }
                }
                karizMetrics.MarkRedisGetNotAvailable(1)
                return Optional.empty()
            }

        } catch (e: Exception) {
            karizMetrics.MarkRedisDelFail(1)
            logger.trace(e) { "There is no resource in pool for redis.get." }
            return Optional.empty()
        }
    }


    fun mget(keys: List<String>): MutableMap<String, Optional<String>> {

        karizMetrics.MarkRedisGetCall(keys.size.toLong())

        val result = keys.map { it to Optional.empty<String>() }.toMap().toMutableMap()

        return try {

            val pool = redisPoolList.randomItem().get()
            pool.resource.use { redis ->

                keys.zip(redis.mget(*keys.toTypedArray())).toMap().forEach { k, v ->
                    if (v != null) {
                        karizMetrics.MarkRedisGetAvailable(1)
                        result[k] = Optional.of(v)
                    } else
                        karizMetrics.MarkRedisGetNotAvailable(1)
                }

                result
            }

        } catch (e: Exception) {
            karizMetrics.MarkRedisGetFail(keys.size.toLong())
            logger.trace(e) { "There is no resource in pool for redis.mget." }
            keys.map { it to Optional.empty<String>() }.toMap().toMutableMap()
        }
    }

    fun set(key: String, value: String): Boolean {

        karizMetrics.MarkRedisSetCall(1)

        val pool = redisPoolList.randomItem().get()
        return try {
            val res = pool.resource.set(key, value)
            if (res == "OK") {
                karizMetrics.MarkRedisSetSuccess(1)
                true
            } else {
                karizMetrics.MarkRedisSetNotSuccess(1)
                false
            }

        } catch (e: Exception) {
            karizMetrics.MarkRedisSetFail(1)
            logger.error { "redis set $e" }
            false
        }
    }


    fun mset(keyValues: List<String>): Boolean {

        val kvSize = keyValues.size.toLong() / 2

        karizMetrics.MarkRedisSetCall(kvSize)

        val pool = redisPoolList.randomItem().get()
        return try {
            pool.resource.use { redis ->
                val res = redis.mset(*keyValues.toTypedArray())
                if (res == "OK") {
                    karizMetrics.MarkRedisSetSuccess(kvSize)
                    true
                } else {
                    karizMetrics.MarkRedisSetNotSuccess(kvSize)
                    false
                }

            }
        } catch (e: Exception) {
            karizMetrics.MarkRedisSetFail(kvSize)
            false
        }
    }


    fun del(key: String): Boolean {

        val pool = redisPoolList.randomItem().get()

        return try {
            val res = pool.resource.del(key)
            if (res == 1L) {
                karizMetrics.MarkRedisDelSuccess(1)
                true
            } else {
                karizMetrics.MarkRedisDelFail(1)
                false
            }

        } catch (e: Exception) {
            karizMetrics.MarkRedisDelFail(1)
            logger.error { "redis del $e" }
            false
        }
    }

    fun expire(key: String, ttl: Long?): Boolean {


        val pool = redisPoolList.randomItem().get()
        return try {
            if (ttl != null) {

                val res = pool.resource.expire(key, ttl.toInt())
                if (res == 1L) {
                    karizMetrics.MarkRedisExpireSuccess(1)
                    true
                } else {
                    karizMetrics.MarkRedisExpireFail(1)
                    false
                }
            } else {
                karizMetrics.MarkRedisExpireFail(1)
                false
            }

        } catch (e: Exception) {
            karizMetrics.MarkRedisExpireFail(1)
            logger.error { "redis del $e" }
            false
        }
    }


}