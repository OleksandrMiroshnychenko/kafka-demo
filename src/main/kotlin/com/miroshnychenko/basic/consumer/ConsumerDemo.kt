package com.miroshnychenko.basic.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

object ConsumerDemo {
    private val log: Logger = LoggerFactory.getLogger(ConsumerDemo::class.java.simpleName)

    @JvmStatic
    fun main(args: Array<String>) {
        val groupId = "kotlin-app"
        val topic = "demo_kotlin"

        val properties = Properties()
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
        properties.setProperty("key.deserializer", StringDeserializer::class.java.name)
        properties.setProperty("value.deserializer", StringDeserializer::class.java.name)
        properties.setProperty("group.id", groupId)
        properties.setProperty("auto.offset.reset", "earliest")

        val consumer = KafkaConsumer<String, String>(properties)

        consumer.subscribe(listOf(topic))

        while (true) {
            log.info("Polling")

            val records = consumer.poll(Duration.ofMillis(1000))

            for (record in records) {
                log.info("Key: ${record.key()}, Value: ${record.value()}")
                log.info("Partition: ${record.partition()}, Offset: ${record.offset()}")
            }
        }
    }
}