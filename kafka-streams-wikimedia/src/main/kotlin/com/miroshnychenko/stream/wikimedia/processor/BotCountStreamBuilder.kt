package com.miroshnychenko.stream.wikimedia.processor

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import java.io.IOException

class BotCountStreamBuilder(private val inputStream: KStream<String, String>) {
    fun setup() {
        inputStream
            .mapValues { changeJson: String? ->
                try {
                    val jsonNode = OBJECT_MAPPER.readTree(changeJson)
                    if (jsonNode["bot"].asBoolean()) {
                        return@mapValues "bot"
                    }
                    return@mapValues "non-bot"
                } catch (e: IOException) {
                    return@mapValues "parse-error"
                }
            }
            .groupBy { _: String?, botOrNot: String -> botOrNot }
            .count(Materialized.`as`(BOT_COUNT_STORE))
            .toStream()
            .mapValues { key: String, value: Long ->
                val kvMap =
                    mapOf(Pair(key, value))
                try {
                    return@mapValues OBJECT_MAPPER.writeValueAsString(kvMap)
                } catch (e: JsonProcessingException) {
                    return@mapValues null
                }
            }
            .to(BOT_COUNT_TOPIC)
    }

    companion object {
        private const val BOT_COUNT_STORE = "bot-count-store"
        private const val BOT_COUNT_TOPIC = "wikimedia.stats.bots"
        private val OBJECT_MAPPER = ObjectMapper()
    }
}