package org.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileProducer.class.getName());
    private static final String KAFKA_ADDRESS_LOCAL = "ubuntu.orb.local:9092";
    private static final String TOPIC_NAME = "file-topic";
    private static final String FILE_PATH = "/Users/ep/Project/kafka-study/practice/src/main/resources/pizza_sample.txt";
    private static final String COMMA_DELIMITER = ",";

    public static void main(String[] args) {
        // kafka producer configuration setting
        Properties properties = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        // 멀티 브로커 환경에서는 여러 서버 설정을 할 수 있다.
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS_LOCAL);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        sendFileMessage(kafkaProducer, TOPIC_NAME, FILE_PATH);


        kafkaProducer.close();
    }

    private static void sendFileMessage(KafkaProducer kafkaProducer, String topicName, String filePath) {
        String line = "";
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(COMMA_DELIMITER);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                for (int i = 1; i < tokens.length; i++) {
                    if (i != tokens.length - 1) {
                        value.append(tokens[i] + ",");
                    } else {
                        value.append(tokens[i]);
                    }
                }

                sendMessage(kafkaProducer, topicName, key, value.toString());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendMessage(KafkaProducer kafkaProducer, String topicName, String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        LOGGER.info("key:{}, value:{}", key, value);

        // KafkaProducer message send
        kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
            if (exception == null) {
                LOGGER.info("\n ###### record metadata receive ##### \n" +
                        "partition: " + recordMetadata.partition() + "\n" +
                        "offset: " + recordMetadata.offset() + "\n" +
                        "timestamp: " + recordMetadata.timestamp() + "\n"
                );
            } else {
                LOGGER.error("exception error from broker " + exception.getMessage());
            }
        });
    }
}
