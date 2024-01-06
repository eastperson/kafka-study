package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeupV2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerWakeupV2.class.getName());
    private static final String KAFKA_ADDRESS_LOCAL = "ubuntu.orb.local:9092";
    private static final String SIMPLE_TOPIC_NAME = "pizza-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();

        // 필수 config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS_LOCAL);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_02");
        // 60 초
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(SIMPLE_TOPIC_NAME));

        // main thread 참조 변수
        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도로 thread 로 kafka consumer wakeup() 메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("main program starts to exit by calling wakeup");
            // wakeup 은 컨슈머를 깨우는 것이다. 그리고 Long poll 을 abort 시킨다.
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        int loopCnt = 0;
        try {
            while (true) {
                // 마지막 Offset 기준으로 다음 메시지를 가져온다
                // 여러개의 레코드를 가져온다(batch)
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                LOGGER.info("######## loopCnt:{}, consumerRecords count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    LOGGER.info("record key:{}, record value:{}, partition:{}, record offset:{}", record.key(), record.value(), record.partition(), record.offset());
                }

                try {
                    LOGGER.info("main thread is sleeping {} ms during while loop", loopCnt*10000);
                    // 10초 + 루프별 10초
                    Thread.sleep(10000 + loopCnt*10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (WakeupException e) {
            LOGGER.error("wakeup exception has been called");
        } finally {
            LOGGER.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }
}
