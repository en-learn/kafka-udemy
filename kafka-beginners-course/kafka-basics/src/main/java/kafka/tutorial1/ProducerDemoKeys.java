package kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

  public static void main(String[] args) throws ExecutionException, InterruptedException {

    final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    // create Producer properties
    Properties properties = new Properties();
    String bootstrapServers = "127.0.0.1:9092";
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {

      // create a producer record

      String topic = "first_topic";
      String value = "hello world" + Integer.toString(i);
      String key = "id_" + Integer.toString(i);

      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

      logger.info("Key: " + key); // log the key
      // key partition
      // 1 0
      // 2 2
      // 3 0
      // 4 2
      // 5 2
      // 6 0
      // 7 2
      // 8 1
      // 9 2

      // send data -- asynchronous
      producer
          .send(
              record,
              new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                  // executes every time a record is successfully sent or an exception is thrown
                  if (e == null) {
                    // the record was successfully sent
                    logger.info(
                        "Received new metadata. \n"
                            + "Topic: "
                            + recordMetadata.topic()
                            + "\n"
                            + "Partition: "
                            + recordMetadata.partition()
                            + "\n"
                            + "Offset: "
                            + recordMetadata.offset()
                            + "\n"
                            + "Timestamp: "
                            + recordMetadata.timestamp()
                            + "\n");
                  } else {
                    logger.error("Error while producing", e);
                  }
                }
              })
          .get(); // block the .send() to make it synchronous - don't do this in production
    }

    // flush data
    producer.flush();
    // flush and close
    producer.close();
  }
}
