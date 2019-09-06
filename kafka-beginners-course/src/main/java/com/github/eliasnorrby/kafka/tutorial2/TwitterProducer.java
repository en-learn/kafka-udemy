package com.github.eliasnorrby.kafka.tutorial2;

import com.github.eliasnorrby.kafka.tutorial2.util.ApplicationProperties;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

  Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

  String consumerKey = ApplicationProperties.INSTANCE.getConsumerKey();
  String consumerSecret = ApplicationProperties.INSTANCE.getConsumerSecret();
  String token = ApplicationProperties.INSTANCE.getToken();
  String secret = ApplicationProperties.INSTANCE.getSecret();

  List<String> terms = Lists.newArrayList("kafka");

  public TwitterProducer() {}

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {

    logger.info("Setup");

    /**
     * Set up your blocking queues: Be sure to size these properly based on expected TPS of your
     * stream
     */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

    // create a twitter client
    Client client = createTwitterClient(msgQueue);

    // Attempts to establish a connection.
    logger.info("Build client, trying to connect...");

    client.connect();

    // create a kafka producer
    KafkaProducer<String, String> producer = createKafkaProducer();

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          logger.info("Stopping application...");
          logger.info("Shutting down client from twitter...");
          client.stop();
          logger.info("Closing producer...");
          producer.close();
          logger.info("Done!");
    }));

    // loop to send tweets to kafka
    // on a different thread, or multiple different threads....
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        logger.info(msg);
        producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
              logger.error("Something bad happened", e);
            }
          }
        });
      }
    }
    logger.info("End of application");
  }

  public Client createTwitterClient(BlockingQueue<String> msgQueue) {

    /**
     * Declare the host you want to connect to, the endpoint, and authentication (basic auth or
     * oauth)
     */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    ClientBuilder builder =
        new ClientBuilder()
            .name("Hosebird-Client-01") // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }

  public KafkaProducer<String, String> createKafkaProducer() {
    // create Producer properties
    Properties properties = new Properties();
    String bootstrapServers = "127.0.0.1:9092";
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    return producer;
  }
}
