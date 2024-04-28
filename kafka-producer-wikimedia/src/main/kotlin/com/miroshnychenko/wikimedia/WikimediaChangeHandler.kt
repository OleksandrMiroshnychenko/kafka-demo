package com.miroshnychenko.wikimedia

import com.launchdarkly.eventsource.EventHandler
import com.launchdarkly.eventsource.MessageEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

class WikimediaChangeHandler(
    private val producer: KafkaProducer<String, String>,
    private val topic: String
) :
    EventHandler {

    private val log = LoggerFactory.getLogger(WikimediaChangeHandler::class.java.simpleName)

    override fun onOpen() {
        // nothing
    }

    override fun onClosed() {
        producer.close()
    }

    override fun onMessage(event: String, message: MessageEvent) {
        log.info(message.data)

        producer.send(ProducerRecord(topic, message.data))
    }

    override fun onComment(comment: String) {
        // nothing
    }

    override fun onError(ex: Throwable) {
        log.error("Error: ", ex)
    }
}