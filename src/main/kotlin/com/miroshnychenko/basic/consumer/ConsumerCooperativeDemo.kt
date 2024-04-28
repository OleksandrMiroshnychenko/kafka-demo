package com.miroshnychenko.basic.consumer

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

object ConsumerCooperativeDemo {
    private val log: Logger = LoggerFactory.getLogger(ConsumerCooperativeDemo::class.java.simpleName)

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
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor::class.java.name)

        val consumer = KafkaConsumer<String, String>(properties)

        val mainThread = Thread.currentThread()
        Runtime.getRuntime().addShutdownHook(Thread {
            log.info("Shutdown through consumer.wakeup()")
            consumer.wakeup()
            mainThread.join()
        })

        consumer.subscribe(listOf(topic))

        try {
            while (true) {
                log.info("Polling")

                val records = consumer.poll(Duration.ofMillis(1000))

                for (record in records) {
                    log.info("Key: ${record.key()}, Value: ${record.value()}")
                    log.info("Partition: ${record.partition()}, Offset: ${record.offset()}")
                }
            }
        } catch (ex: WakeupException) {
            log.info("Starting shut down...")
        } catch (ex: Exception) {
            log.error("Unexpected exception", ex)
        } finally {
            consumer.close()
            log.info("Consumer closed")
        }
    }
}