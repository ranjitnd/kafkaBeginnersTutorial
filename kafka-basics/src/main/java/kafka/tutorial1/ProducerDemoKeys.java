package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "localhost:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {
            // create a producer record
            String topic = "first_topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "id_"+ Integer.toString(i);
            final ProducerRecord<String, String>  record =
                    new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key = " + key);
            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Record Meta Data: " +
                                "\n Topic: " + recordMetadata.topic() +
                                "\n Partition: " + recordMetadata.partition() +
                                "\n Offset: " + recordMetadata.offset());
                    } else {
                        logger.error("Error while sending: ", e);
                    }
                }
            }).get(); //synchronous send

        }

        // flush producer
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
