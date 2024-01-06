package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SimpleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class.getName());
    private static final String KAFKA_ADDRESS_LOCAL = "ubuntu.orb.local:9092";
    private static final String SIMPLE_TOPIC_NAME = "simple-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();

        // 필수 config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS_LOCAL);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01");
//        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
//        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "90000");
//        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(SIMPLE_TOPIC_NAME));
        while (true) {
            // 마지막 Offset 기준으로 다음 메시지를 가져온다
            // 여러개의 레코드를 가져온다(batch)
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                LOGGER.info("record key:{}, record value:{}, partition:{}", record.key(), record.value(), record.partition());
            }
        }

        // kafkaConsumer.close();
    }
}
