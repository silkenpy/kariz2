package ir.rkr.kariz.caffeine


import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Expiry
import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.kariz.kafka.KafkaConnector
import ir.rkr.kariz.netty.Command
import ir.rkr.kariz.util.KarizMetrics
import mu.KotlinLogging
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

class ByteArrayKey(valueStr: String) {

    val value = valueStr.toByteArray(StandardCharsets.US_ASCII)

    override fun hashCode(): Int {
        return value.contentHashCode()
    }

    override fun equals(other: Any?): Boolean =
        (this === other) || (other is ByteArrayKey && this.value.contentEquals(other.value))
}

class Entry(val value: ByteArrayKey, val ttl: Long)


class CaffeineBuilder(val kafka: KafkaConnector, config: Config, val karizMetrics: KarizMetrics) {

    private val logger = KotlinLogging.logger {}
    private val gson = GsonBuilder().disableHtmlEscaping().create()
    private val cache: Cache<ByteArrayKey, Entry>

    fun String.asciiBytes(): ByteArray =
            this.toByteArray(StandardCharsets.US_ASCII)

    fun String.asciiBytesKey(): ByteArrayKey =
            ByteArrayKey(this)

    init {

        cache = Caffeine.newBuilder().expireAfter(object : Expiry<ByteArrayKey, Entry> {

            override fun expireAfterCreate(key: ByteArrayKey, value: Entry, currentTime: Long): Long {
                return TimeUnit.SECONDS.toNanos(value.ttl)
            }

            override fun expireAfterUpdate(key: ByteArrayKey, value: Entry, currentTime: Long, currentDuration: Long): Long {
                return TimeUnit.SECONDS.toNanos(value.ttl)
            }

            override fun expireAfterRead(key: ByteArrayKey, value: Entry, currentTime: Long, currentDuration: Long): Long {
                return currentDuration
            }
        }).removalListener<ByteArrayKey, Entry> { k, v, c -> }
//                .recordStats()
                .build<ByteArrayKey, Entry>()


        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({

            val commands = kafka.get()

            if (commands.size > 0) {
                commands.forEach { t, u ->

                    val parsed = gson.fromJson(u, Command::class.java)
                    when (parsed.cmd) {

                        "set" -> set(parsed.key, parsed.value!!, parsed.ttl, parsed.time)

                        "del" -> del(parsed.key)

                        "expire" -> expire(parsed.key, parsed.ttl)

                        else -> {logger.error { parsed.toString() }
                        }
                    }
                }

                kafka.commit()
            }

        }, 0, 100, TimeUnit.MILLISECONDS)

//        karizMetrics.addGauge("CaffeineStats", Supplier { cache.stats() })
        karizMetrics.addGauge("CaffeineEstimatedSize", Supplier { cache.estimatedSize() })
    }


    fun set(key: String, value: String, ttl: Long?, time: Long): Boolean {

        karizMetrics.MarkCaffeineSetCall(1)
        return try {
            if (ttl == null) {
                cache.put(key.asciiBytesKey(), Entry(value.asciiBytesKey(), Long.MAX_VALUE))
                karizMetrics.MarkCaffeineSetWithoutTTL(1)
            } else {
                val remained = ((ttl * 1000 + time) - System.currentTimeMillis()) / 1000

                if (remained > 0) {
                    cache.put(key.asciiBytesKey(), Entry(value.asciiBytesKey(), remained))
                    karizMetrics.MarkCaffeineSetWithTTL(1)
                } else
                    karizMetrics.MarkCaffeineSetElapsedTTL(1)
            }

            karizMetrics.MarkCaffeineSetSuccess(1)
            true

        } catch (e: Exception) {
            karizMetrics.MarkCaffeineSetFail(1)
            logger.error(e) { "Error $e" }
            false
        }

    }

    fun get(key: String): Optional<String> {

        karizMetrics.MarkCaffeineGetCall(1)

        return try {

            val entry = cache.getIfPresent(key.asciiBytesKey())

            return if (entry != null) {
                karizMetrics.MarkCaffeineGetAvailable(1)
                Optional.of(String(entry.value.value,StandardCharsets.US_ASCII))
            } else {
                karizMetrics.MarkCaffeineGetNotAvailable(1)
                Optional.empty()
            }

        } catch (e: Exception) {
            karizMetrics.MarkCaffeineGetFail(1)
            Optional.empty()
        }
    }

    fun del(key: String): Boolean {

        return try {
            cache.invalidate(key.asciiBytesKey())
            karizMetrics.MarkCaffeineDelSuccess(1)
            true

        } catch (e: Exception) {
            karizMetrics.MarkCaffeineDelFail(1)
            false
        }
    }

    fun expire(key: String, ttl: Long?) {

        try {
            if (ttl != null) {
                val keyBytes = key.asciiBytesKey()
                cache.put(keyBytes, Entry(cache.getIfPresent(keyBytes)!!.value, ttl))
                karizMetrics.MarkCaffeineExpireSuccess(1)
            }

        } catch (e: Exception) {
            karizMetrics.MarkCaffeineExpireFail(1)

        }
    }

}
