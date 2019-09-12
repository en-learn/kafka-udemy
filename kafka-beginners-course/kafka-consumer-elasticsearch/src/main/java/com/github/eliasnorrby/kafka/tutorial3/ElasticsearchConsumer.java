package com.github.eliasnorrby.kafka.tutorial3;

import com.github.eliasnorrby.kafka.tutorial3.util.ApplicationProperties;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchConsumer {

  private static JsonParser jsonParser = new JsonParser();

  public static RestHighLevelClient createClient() {
    String hostname = ApplicationProperties.INSTANCE.getHostname();
    String username = ApplicationProperties.INSTANCE.getUsername();
    String password = ApplicationProperties.INSTANCE.getPassword();

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder =
        RestClient.builder(new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                  @Override
                  public HttpAsyncClientBuilder customizeHttpClient(
                      HttpAsyncClientBuilder httpAsyncClientBuilder) {
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(
                        credentialsProvider);
                  }
                });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public static KafkaConsumer<String, String> createConsumer(String topic) {

    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "kafka-demo-elasticsearch";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    consumer.subscribe(Arrays.asList(topic));

    return consumer;
  }

  private static String extractIdFromTweet(String tweetJson) {
    // gson library
    return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
  }

  public static void main(String[] args) throws IOException {
    Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());
    RestHighLevelClient client = createClient();

    KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        // 2 strategies for generating IDs
        // kafka generic ID
        // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

        // twitter feed specific id
        String id = extractIdFromTweet(record.value());

        // where we insert data into Elasticsearch
        // deprecated style
        // IndexRequest indexRequest = new IndexRequest(
        //   "twitter","tweets",
        //   id
        // ).source(record.value(), XContentType.JSON);
        IndexRequest indexRequest =
            new IndexRequest("twitter").id(id).source(record.value(), XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

        logger.info(indexResponse.getId());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    // close the client gracefully
    // client.close();
  }
}
