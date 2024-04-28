package com.miroshnychenko.stream.wikimedia.processor

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.*
import java.io.IOException
import java.time.Duration

class WebsiteCountStreamBuilder(private val inputStream: KStream<String, String>) {
    fun setup() {
        val timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L))
        inputStream
            .selectKey { _: String?, changeJson: String? ->
                try {
                    val jsonNode = OBJECT_MAPPER.readTree(changeJson)
                    return@selectKey jsonNode["server_name"].asText()
                } catch (e: IOException) {
                    return@selectKey "parse-error"
                }
            }
            .groupByKey()
            .windowedBy(timeWindows)
            .count(Materialized.`as`(WEBSITE_COUNT_STORE))
            .toStream()
            .mapValues { key: Windowed<String>, value: Long ->
                val kvMap = mapOf(
                    Pair("website", key.key()),
                    Pair("count", value)
                )
                try {
                    return@mapValues OBJECT_MAPPER.writeValueAsString(kvMap)
                } catch (e: JsonProcessingException) {
                    return@mapValues null
                }
            }
            .to(
                WEBSITE_COUNT_TOPIC, Produced.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String::class.java, timeWindows.size()),
                    Serdes.String()
                )
            )
    }

    companion object {
        private const val WEBSITE_COUNT_STORE = "website-count-store"
        private const val WEBSITE_COUNT_TOPIC = "wikimedia.stats.website"
        private val OBJECT_MAPPER = ObjectMapper()
    }
}