package com.miroshnychenko.stream.wikimedia

import com.miroshnychenko.stream.wikimedia.processor.BotCountStreamBuilder
import com.miroshnychenko.stream.wikimedia.processor.EventCountTimeseriesBuilder
import com.miroshnychenko.stream.wikimedia.processor.WebsiteCountStreamBuilder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.slf4j.LoggerFactory
import java.util.*

object WikimediaStreamsApp {
    private val log = LoggerFactory.getLogger(WikimediaStreamsApp::class.java)
    private val properties = Properties()
    private const val INPUT_TOPIC = "wikimedia.recent.change"

    init {
        properties[StreamsConfig.APPLICATION_ID_CONFIG] = "wikimedia-stats-application"
        properties[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        properties[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        properties[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val builder = StreamsBuilder()
        val changeJsonStream = builder.stream<String, String>(INPUT_TOPIC)

        val botCountStreamBuilder = BotCountStreamBuilder(changeJsonStream)
        botCountStreamBuilder.setup()

        val websiteCountStreamBuilder = WebsiteCountStreamBuilder(changeJsonStream)
        websiteCountStreamBuilder.setup()

        val eventCountTimeseriesBuilder = EventCountTimeseriesBuilder(changeJsonStream)
        eventCountTimeseriesBuilder.setup()

        val appTopology = builder.build()
        log.info("Topology: {}", appTopology.describe())
        val streams = KafkaStreams(appTopology, properties)
        streams.start()
    }
}