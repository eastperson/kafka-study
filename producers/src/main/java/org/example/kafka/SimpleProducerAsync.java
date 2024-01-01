package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerAsync {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());
    private static final String KAFKA_ADDRESS_LOCAL = "ubuntu.orb.local:9092";
    private static final String SIMPLE_TOPIC_NAME = "simple-topic";

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

        // ProducerRecord 객체 생성(key, message)
        // key: id-001, value: "hello world"
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SIMPLE_TOPIC_NAME, "id-001", "hello world2");

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

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
