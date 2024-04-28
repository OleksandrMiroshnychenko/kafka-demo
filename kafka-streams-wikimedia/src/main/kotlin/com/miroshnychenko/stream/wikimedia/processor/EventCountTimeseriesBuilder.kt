package com.miroshnychenko.stream.wikimedia.processor

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.*
import java.time.Duration


class EventCountTimeseriesBuilder(private val inputStream: KStream<String, String>) {
    fun setup() {
        val timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10))
        inputStream
            .selectKey { _: String?, _: String? -> "key-to-group" }
            .groupByKey()
            .windowedBy(timeWindows)
            .count(Materialized.`as`(TIMESERIES_STORE))
            .toStream()
            .mapValues { readOnlyKey: Windowed<String>, value: Long ->
                val kvMap = mapOf(
                    Pair("start_time", readOnlyKey.window().startTime().toString()),
                    Pair("end_time", readOnlyKey.window().endTime().toString()),
                    Pair("window_size", timeWindows.size()),
                    Pair("event_count", value)
                )
                try {
                    return@mapValues OBJECT_MAPPER.writeValueAsString(kvMap)
                } catch (e: JsonProcessingException) {
                    return@mapValues null
                }
            }
            .to(
                TIMESERIES_TOPIC, Produced.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String::class.java, timeWindows.size()),
                    Serdes.String()
                )
            )
    }

    companion object {
        private const val TIMESERIES_TOPIC = "wikimedia.stats.timeseries"
        private const val TIMESERIES_STORE = "event-count-store"
        private val OBJECT_MAPPER = ObjectMapper()
    }
}