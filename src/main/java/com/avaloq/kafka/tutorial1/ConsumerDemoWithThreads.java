package com.avaloq.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {}

    private void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
        String bootstrapServers = "localhost:9092";
        String topic = "first_topic";
        String groupId = "my-2-application";

        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread ( () -> {
            logger.info("Caught Shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutDown();
            try {
                logger.info("latch await 2 started=====");
                latch.await();
                logger.info("latch await 2 executed=====");

            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                logger.info("Application is closing 3 !!");
            }
            logger.info("Application has exited");
        }
        ));

        try {
            logger.info("latch await started=====");
            latch.await();
            logger.info("latch await executed=====");
        } catch (InterruptedException e) {
            logger.error("Application was interrupted!", e);
        } finally {
            logger.info("Application is closing !!");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            // create producer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // create Kafka Consumer
            this.consumer = new KafkaConsumer<String, String>(properties);
            // Consumer Subscribe
            this.consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll for new Data
            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records
                    ) {
                        logger.info(("key: " + record.key() + " value: " + record.value()));
                        logger.info(("partition: " + record.partition() + " offset: " + record.offset()));
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown() {
            // consumer.wakeup() will interrupt the consumer.poll() method
            // it throws the WakeUpException
            consumer.wakeup();
        }
    }
}
