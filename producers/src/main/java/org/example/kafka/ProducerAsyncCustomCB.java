package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAsyncCustomCB {
    private static final String KAFKA_ADDRESS_LOCAL = "ubuntu.orb.local:9092";
    private static final String MULTIPART_TOPIC_NAME = "multipart-topic-test";

    public static void main(String[] args) {
        // kafka producer configuration setting
        Properties properties = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class
        // 멀티 브로커 환경에서는 여러 서버 설정을 할 수 있다.
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS_LOCAL);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int seq = 0; seq < 20; seq++) {
            // ProducerRecord 객체 생성(key, message)
            // key: id-001, value: "hello world"
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(MULTIPART_TOPIC_NAME, seq, "hello world " + seq);

            // KafkaProducer message send
            kafkaProducer.send(producerRecord, new CustomCallback(seq));
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
