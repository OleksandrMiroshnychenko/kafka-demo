package com.miroshnychenko.basic.producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties

object ProducerCallbackDemo {
    private val log: Logger = LoggerFactory.getLogger(ProducerCallbackDemo::class.java.simpleName)

    @JvmStatic
    fun main(args: Array<String>) {
        val properties: Properties = Properties()
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
        properties.setProperty("key.serializer", StringSerializer::class.java.name)
        properties.setProperty("value.serializer", StringSerializer::class.java.name)
//        properties.setProperty("batch.size", "10")
//        properties.setProperty("partitioner.class", RoundRobinPartitioner::class.java.name)

        val producer: KafkaProducer<String, String> = KafkaProducer(properties)

        for (j in 0..9) {
            for (i in 0..29) {
                val record: ProducerRecord<String, String> = ProducerRecord("demo_kotlin", "hello $i")

                producer.send(record) { meta, ex ->
                    if (ex == null) {
                        log.info(
                            "Received new metadata:\n"
                                    + "Topic: " + meta.topic() + "\n"
                                    + "Partition: " + meta.partition() + "\n"
                                    + "Offset: " + meta.offset() + "\n"
                                    + "Timestamp: " + meta.timestamp() + "\n"
                        )
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