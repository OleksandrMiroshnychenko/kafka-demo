package com.miroshnychenko.basic.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties

object ProducerDemo {
    private val log: Logger = LoggerFactory.getLogger(ProducerDemo::class.java.simpleName)

    @JvmStatic
    fun main(args: Array<String>) {
        val properties: Properties = Properties()
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
        properties.setProperty("key.serializer", StringSerializer::class.java.name)
        properties.setProperty("value.serializer", StringSerializer::class.java.name)

        val producer: KafkaProducer<String, String> = KafkaProducer(properties)
        val record: ProducerRecord<String, String> = ProducerRecord("demo_kotlin", "hello")

        producer.send(record)

        producer.flush()
        producer.close()
    }
}