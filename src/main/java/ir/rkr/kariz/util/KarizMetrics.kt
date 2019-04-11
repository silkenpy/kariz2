package ir.rkr.kariz.util

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

    val CaffeineSetCall = metricRegistry.meter("CaffeineSetCall")
    val CaffeineSetWithTTL = metricRegistry.meter("CaffeineSetWithTTL")
    val CaffeineSetWithoutTTL = metricRegistry.meter("CaffeineSetWithoutTTL")
    val CaffeineSetElapsedTTL = metricRegistry.meter("CaffeineSetElapsedTTL")
    val CaffeineSetSuccess = metricRegistry.meter("CaffeineSetSuccess")
    val CaffeineSetFail = metricRegistry.meter("CaffeineSetFail")

    val CaffeineGetCall = metricRegistry.meter("CaffeineGetCall")
    val CaffeineGetAvailable = metricRegistry.meter("CaffeineGetAvailable")
    val CaffeineGetNotAvailable = metricRegistry.meter("CaffeineGetNotAvailable")
    val CaffeineGetFail = metricRegistry.meter("CaffeineGetFail")

    val CaffeineDelSuccess = metricRegistry.meter("CaffeineDelSuccess")
    val CaffeineDelFail = metricRegistry.meter("CaffeineDelFail")

    val CaffeineExpireSuccess = metricRegistry.meter("CaffeineExpireSuccess")
    val CaffeineExpireFail = metricRegistry.meter("CaffeineExpireFail")


    val UrlBatches = metricRegistry.meter("UrlBatches")
    val CheckUrl = metricRegistry.meter("CheckUrl")
    val UrlInCaffeine = metricRegistry.meter("UrlInCaffeine")
    val UrlNotInCaffeine = metricRegistry.meter("UrlNotInCaffeine")

    val TagBatches = metricRegistry.meter("TagBatches")
    val CheckTag = metricRegistry.meter("CheckTag")
    val TagInCaffeine = metricRegistry.meter("TagInCaffeine")
    val TagNotInCaffeine = metricRegistry.meter("TagNotInCaffeine")

    val UsrBatches = metricRegistry.meter("UsrBatches")
    val CheckUsr = metricRegistry.meter("CheckUsr")
    val UsrInCaffeine = metricRegistry.meter("UsrInCaffeine")
    val UsrNotInCaffeine = metricRegistry.meter("UsrNotInCaffeine")


    fun MarkNettyRequests(l: Long = 1) = NettyRequests.mark(l)

    fun MarkKafkaGetFail(l: Long = 1) = KafkaGetFail.mark(l)
    fun MarkKafkaGetRecords(l: Long = 1) = KafkaGetRecords.mark(l)
    fun MarkKafkaGetDuplicate(l: Long = 1) = KafkaGetDuplicate.mark(l)

    fun MarkKafkaPutCall(l: Long = 1) = KafkaPutCall.mark(l)
    fun MarkKafkaPutFail(l: Long = 1) = KafkaPutFail.mark(l)
    fun MarkKafkaPutRecords(l: Long = 1) = KafkaPutRecords.mark(l)

    fun MarkCaffeineSetCall(l: Long = 1) = CaffeineSetCall.mark(l)
    fun MarkCaffeineSetWithTTL(l: Long = 1) = CaffeineSetWithTTL.mark(l)
    fun MarkCaffeineSetWithoutTTL(l: Long = 1) = CaffeineSetWithoutTTL.mark(l)
    fun MarkCaffeineSetElapsedTTL(l: Long = 1) = CaffeineSetElapsedTTL.mark(l)
    fun MarkCaffeineSetSuccess(l: Long = 1) = CaffeineSetSuccess.mark(l)
    fun MarkCaffeineSetFail(l: Long = 1) = CaffeineSetFail.mark(l)

    fun MarkCaffeineGetCall(l: Long = 1) = CaffeineGetCall.mark(l)
    fun MarkCaffeineGetAvailable(l: Long = 1) = CaffeineGetAvailable.mark(l)
    fun MarkCaffeineGetNotAvailable(l: Long = 1) = CaffeineGetNotAvailable.mark(l)
    fun MarkCaffeineGetFail(l: Long = 1) = CaffeineGetFail.mark(l)

    fun MarkCaffeineDelSuccess(l: Long = 1) = CaffeineDelSuccess.mark(l)
    fun MarkCaffeineDelFail(l: Long = 1) = CaffeineDelFail.mark(l)

    fun MarkCaffeineExpireSuccess(l: Long = 1) = CaffeineExpireSuccess.mark(l)
    fun MarkCaffeineExpireFail(l: Long = 1) = CaffeineExpireFail.mark(l)

    fun MarkUrlBatches(l: Long = 1) = UrlBatches.mark(l)
    fun MarkCheckUrl(l: Long = 1) = CheckUrl.mark(l)
    fun MarkUrlInCaffeine(l: Long = 1) = UrlInCaffeine.mark(l)
    fun MarkUrlNotInCaffeine(l: Long = 1) = UrlNotInCaffeine.mark(l)

    fun MarkTagBatches(l: Long = 1) = TagBatches.mark(l)
    fun MarkCheckTag(l: Long = 1) = CheckTag.mark(l)
    fun MarkTagInCaffeine(l: Long = 1) = TagInCaffeine.mark(l)
    fun MarkTagNotInCaffeine(l: Long = 1) = TagNotInCaffeine.mark(l)


    fun MarkUsrBatches(l: Long = 1) = UsrBatches.mark(l)
    fun MarkCheckUsr(l: Long = 1) = CheckUsr.mark(l)
    fun MarkUsrInCaffeine(l: Long = 1) = UsrInCaffeine.mark(l)
    fun MarkUsrNotInCaffeine(l: Long = 1) = UsrNotInCaffeine.mark(l)



    fun <T> addGauge(name: String, supplier: Supplier<T>) = metricRegistry.register(name, Gauge<T> { supplier.get() })

    private fun sortMetersByCount(meters: Map<String, Meter>) =
            meters.toList().sortedBy { it.second.count }.reversed()
                    .map { Pair(it.first, it.second.toPojo()) }.toMap()

    private fun Meter.toPojo() = MeterPojo(count, meanRate, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate)

    fun getInfo() = ServerInfo(metricRegistry.gauges.mapValues { it.value.value },
            sortMetersByCount(metricRegistry.meters))


}

