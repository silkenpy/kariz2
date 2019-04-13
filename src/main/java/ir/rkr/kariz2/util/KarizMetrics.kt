package ir.rkr.kariz2.util

import com.codahale.metrics.Gauge
import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import java.util.function.Supplier


data class MeterPojo(val count: Long,
                     val rate: Double,
                     val oneMinuteRate: Double,
                     val fiveMinuteRate: Double,
                     val fifteenMinuteRate: Double)

data class ServerInfo(val gauges: Map<String, Any>, val meters: Map<String, MeterPojo>)

class KarizMetrics {

    val metricRegistry = MetricRegistry()

    val NettyRequests = metricRegistry.meter("nettyRequests")

    val KafkaGetCall = metricRegistry.meter("kafkaGetCall")
    val KafkaGetFail = metricRegistry.meter("kafkaGetFail")
    val KafkaGetRecords = metricRegistry.meter("kafkaGetRecords")
    val KafkaGetDuplicate = metricRegistry.meter("KafkaGetDuplicate")

    val KafkaPutCall = metricRegistry.meter("KafkaPutCall")
    val KafkaPutFail = metricRegistry.meter("KafkaPutFail")
    val KafkaPutRecords = metricRegistry.meter("kafkaPutRecords")

    val RedisSetCall = metricRegistry.meter("RedisSetCall")
    val RedisSetWithTTL = metricRegistry.meter("RedisSetWithTTL")
    val RedisSetWithoutTTL = metricRegistry.meter("RedisSetWithoutTTL")
    val RedisSetElapsedTTL = metricRegistry.meter("RedisSetElapsedTTL")
    val RedisSetSuccess = metricRegistry.meter("RedisSetSuccess")
    val RedisSetNotSuccess = metricRegistry.meter("RedisSetNotSuccess")
    val RedisSetFail = metricRegistry.meter("RedisSetFail")

    val RedisGetCall = metricRegistry.meter("RedisGetCall")
    val RedisGetAvailable = metricRegistry.meter("RedisGetAvailable")
    val RedisGetNotAvailable = metricRegistry.meter("RedisGetNotAvailable")
    val RedisGetFail = metricRegistry.meter("RedisGetFail")

    val RedisDelSuccess = metricRegistry.meter("RedisDelSuccess")
    val RedisDelFail = metricRegistry.meter("RedisDelFail")

    val RedisExpireSuccess = metricRegistry.meter("RedisExpireSuccess")
    val RedisExpireFail = metricRegistry.meter("RedisExpireFail")


    val UrlBatches = metricRegistry.meter("UrlBatches")
    val CheckUrl = metricRegistry.meter("CheckUrl")
    val UrlInRedis = metricRegistry.meter("UrlInRedis")
    val UrlNotInRedis = metricRegistry.meter("UrlNotInRedis")

    val TagBatches = metricRegistry.meter("TagBatches")
    val CheckTag = metricRegistry.meter("CheckTag")
    val TagInRedis = metricRegistry.meter("TagInRedis")
    val TagNotInRedis = metricRegistry.meter("TagNotInRedis")

    val UsrBatches = metricRegistry.meter("UsrBatches")
    val CheckUsr = metricRegistry.meter("CheckUsr")
    val UsrInRedis = metricRegistry.meter("UsrInRedis")
    val UsrNotInRedis = metricRegistry.meter("UsrNotInRedis")


    fun MarkNettyRequests(l: Long = 1) = NettyRequests.mark(l)

    fun MarkKafkaGetCall(l: Long = 1) = KafkaGetCall.mark(l)
    fun MarkKafkaGetFail(l: Long = 1) = KafkaGetFail.mark(l)
    fun MarkKafkaGetRecords(l: Long = 1) = KafkaGetRecords.mark(l)
    fun MarkKafkaGetDuplicate(l: Long = 1) = KafkaGetDuplicate.mark(l)

    fun MarkKafkaPutCall(l: Long = 1) = KafkaPutCall.mark(l)
    fun MarkKafkaPutFail(l: Long = 1) = KafkaPutFail.mark(l)
    fun MarkKafkaPutRecords(l: Long = 1) = KafkaPutRecords.mark(l)

    fun MarkRedisSetCall(l: Long = 1) = RedisSetCall.mark(l)
    fun MarkRedisSetWithTTL(l: Long = 1) = RedisSetWithTTL.mark(l)
    fun MarkRedisSetWithoutTTL(l: Long = 1) = RedisSetWithoutTTL.mark(l)
    fun MarkRedisSetElapsedTTL(l: Long = 1) = RedisSetElapsedTTL.mark(l)
    fun MarkRedisSetSuccess(l: Long = 1) = RedisSetSuccess.mark(l)
    fun MarkRedisSetNotSuccess(l: Long = 1) = RedisSetNotSuccess.mark(l)
    fun MarkRedisSetFail(l: Long = 1) = RedisSetFail.mark(l)

    fun MarkRedisGetCall(l: Long = 1) = RedisGetCall.mark(l)
    fun MarkRedisGetAvailable(l: Long = 1) = RedisGetAvailable.mark(l)
    fun MarkRedisGetNotAvailable(l: Long = 1) = RedisGetNotAvailable.mark(l)
    fun MarkRedisGetFail(l: Long = 1) = RedisGetFail.mark(l)

    fun MarkRedisDelSuccess(l: Long = 1) = RedisDelSuccess.mark(l)
    fun MarkRedisDelFail(l: Long = 1) = RedisDelFail.mark(l)

    fun MarkRedisExpireSuccess(l: Long = 1) = RedisExpireSuccess.mark(l)
    fun MarkRedisExpireFail(l: Long = 1) = RedisExpireFail.mark(l)

    fun MarkUrlBatches(l: Long = 1) = UrlBatches.mark(l)
    fun MarkCheckUrl(l: Long = 1) = CheckUrl.mark(l)
    fun MarkUrlInRedis(l: Long = 1) = UrlInRedis.mark(l)
    fun MarkUrlNotInRedis(l: Long = 1) = UrlNotInRedis.mark(l)

    fun MarkTagBatches(l: Long = 1) = TagBatches.mark(l)
    fun MarkCheckTag(l: Long = 1) = CheckTag.mark(l)
    fun MarkTagInRedis(l: Long = 1) = TagInRedis.mark(l)
    fun MarkTagNotInRedis(l: Long = 1) = TagNotInRedis.mark(l)


    fun MarkUsrBatches(l: Long = 1) = UsrBatches.mark(l)
    fun MarkCheckUsr(l: Long = 1) = CheckUsr.mark(l)
    fun MarkUsrInRedis(l: Long = 1) = UsrInRedis.mark(l)
    fun MarkUsrNotInRedis(l: Long = 1) = UsrNotInRedis.mark(l)



    fun <T> addGauge(name: String, supplier: Supplier<T>) = metricRegistry.register(name, Gauge<T> { supplier.get() })

    private fun sortMetersByCount(meters: Map<String, Meter>) =
            meters.toList().sortedBy { it.second.count }.reversed()
                    .map { Pair(it.first, it.second.toPojo()) }.toMap()

    private fun Meter.toPojo() = MeterPojo(count, meanRate, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate)

    fun getInfo() = ServerInfo(metricRegistry.gauges.mapValues { it.value.value },
            sortMetersByCount(metricRegistry.meters))


}

