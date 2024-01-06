package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {

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
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(SIMPLE_TOPIC_NAME, "id-001", "hello world");

        // KafkaProducer message send
        kafkaProducer.send(producerRecord);

        // batch 가 돌기 때문에 flush 를 써줘야 한다.
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
