package com.miroshnychenko.wikimedia

import com.launchdarkly.eventsource.EventSource
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URI
import java.util.*
import java.util.concurrent.TimeUnit

object WikimediaChangesProducer {

    private val log: Logger = LoggerFactory.getLogger(WikimediaChangesProducer::class.java.simpleName)

    @JvmStatic
    fun main(args: Array<String>) {
        val properties = Properties()
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
        properties.setProperty("key.serializer", StringSerializer::class.java.name)
        properties.setProperty("value.serializer", StringSerializer::class.java.name)

        // Safe producer settings
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString())

        // High throughput producer settings
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32 * 1024).toString())
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")

        val producer: KafkaProducer<String, String> = KafkaProducer(properties)

        val topic = "wikimedia.recent.change"
        val eventHandler = WikimediaChangeHandler(producer, topic)
        val url = "https://stream.wikimedia.org/v2/stream/recentchange"
        val eventSource = EventSource.Builder(eventHandler, URI.create(url)).build()

        eventSource.start()

        TimeUnit.SECONDS.sleep(15)
    }
}