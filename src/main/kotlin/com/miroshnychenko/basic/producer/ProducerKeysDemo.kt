package com.miroshnychenko.basic.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties

object ProducerKeysDemo {
    private val log: Logger = LoggerFactory.getLogger(ProducerKeysDemo::class.java.simpleName)

    @JvmStatic
    fun main(args: Array<String>) {
        val properties = Properties()
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
        properties.setProperty("key.serializer", StringSerializer::class.java.name)
        properties.setProperty("value.serializer", StringSerializer::class.java.name)

        val producer: KafkaProducer<String, String> = KafkaProducer(properties)

        for (j in 0..1) {
            for (i in 0..9) {
                val topic = "demo_kotlin"
                val key = "id_$i"
                val value = "hello $i"
                val record: ProducerRecord<String, String> = ProducerRecord(topic, key, value)

                producer.send(record) { meta, ex ->
                    if (ex == null) {
                        log.info("Key: $key Partition: ${meta.partition()}")
                    } else {
                        log.error("Error while producing: ", ex)
                    }
                }
            }
            Thread.sleep(500)
        }

        producer.flush()
        producer.close()
    }
}