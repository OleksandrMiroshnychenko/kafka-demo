package com.miroshnychenko.opensearch

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestClient
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.common.xcontent.XContentType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URI
import java.time.Duration
import java.util.*


object OpenSearchConsumer {

    private const val WIKIMEDIA = "wikimedia"
    private const val WIKIMEDIA_TOPIC = "wikimedia.recent.change"

    private val log: Logger = LoggerFactory.getLogger(OpenSearchConsumer::class.java.simpleName)

    @JvmStatic
    fun main(args: Array<String>) {
        val openSearchClient = createOpenSearchClient()
        val consumer = createKafkaConsumer()

        addShutdownHook(consumer)

        try {
            if (!openSearchClient.indices().exists(GetIndexRequest(WIKIMEDIA), RequestOptions.DEFAULT)) {
                openSearchClient.indices().create(CreateIndexRequest(WIKIMEDIA), RequestOptions.DEFAULT)
            }

            consumer.subscribe(mutableListOf(WIKIMEDIA_TOPIC))

            while (true) {
                val records = consumer.poll(Duration.ofSeconds(3))
                log.info("Received ${records.count()} record(s)")

                val bulkRequest = BulkRequest()
                for (record in records) {

                    try {
                        val indexRequest = IndexRequest(WIKIMEDIA).source(record.value(), XContentType.JSON)
                            .id(extractIdFromWikimediaChange(record.value()))
                        bulkRequest.add(indexRequest)
                    } catch (ex: Exception) {
                        log.info("Alone opensearch ex, ignoring...")
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    val bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT)
                    log.info("Inserted ${bulkResponse.items.size} record(s)")
                    consumer.commitSync()
                    log.info("Offsets have been committed")
                }
            }

        } catch (ex: WakeupException) {
            log.info("Woke up")
        } finally {
            openSearchClient.close()
            consumer.close()
            log.info("Resources are gracefully closed")
        }
    }

    private fun extractIdFromWikimediaChange(json: String): String {
        return JsonParser.parseString(json)
            .asJsonObject.get("meta")
            .asJsonObject.get("id")
            .asString
    }

    private fun createOpenSearchClient(): RestHighLevelClient {
        val connString = "http://localhost:9200"

        // String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        val restHighLevelClient: RestHighLevelClient
        val connUri: URI = URI.create(connString)
        // extract login information if it exists
        val userInfo = connUri.getUserInfo()

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient =
                RestHighLevelClient(RestClient.builder(HttpHost(connUri.host, connUri.port, "http")))
        } else {
            // REST client with security
            val auth = userInfo.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()

            val cp: CredentialsProvider = BasicCredentialsProvider()
            cp.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(auth[0], auth[1]))

            restHighLevelClient = RestHighLevelClient(
                RestClient.builder(HttpHost(connUri.host, connUri.port, connUri.scheme))
                    .setHttpClientConfigCallback { httpAsyncClientBuilder: HttpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                            .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy())
                    }
            )
        }

        return restHighLevelClient
    }

    private fun createKafkaConsumer(): KafkaConsumer<String, String> {
        val groupId = "consumer-opensearch-demo"

        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

        return KafkaConsumer(properties)
    }

    private fun addShutdownHook(consumer: KafkaConsumer<String, String>) {
        val mainThread = Thread.currentThread()
        Runtime.getRuntime().addShutdownHook(Thread {
            log.info("Shutdown through consumer.wakeup()")
            consumer.wakeup()
            mainThread.join()
        })
    }
}