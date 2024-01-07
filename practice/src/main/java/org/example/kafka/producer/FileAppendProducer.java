package org.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.kafka.event.EventHandler;
import org.example.kafka.event.FileEventHandler;
import org.example.kafka.event.FileEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileAppendProducer.class.getName());
    private static final String KAFKA_ADDRESS_LOCAL = "ubuntu.orb.local:9092";
    private static final String TOPIC_NAME = "file-topic";
    private static final String FILE_PATH = "/Users/ep/Project/kafka-study/practice/src/main/resources/pizza_append.txt";

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS_LOCAL);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        File file = new File(FILE_PATH);
        boolean sync = false;
        EventHandler eventHandler = new FileEventHandler(kafkaProducer, TOPIC_NAME, sync);
        FileEventSource fileEventSource = new FileEventSource(10_000, file, eventHandler);
        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } finally {
            kafkaProducer.close();
        }
    }
}
