package org.example.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerPartitionAssignSeek {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerPartitionAssignSeek.class.getName());
    private static final String KAFKA_ADDRESS_LOCAL = "ubuntu.orb.local:9092";
    private static final String SIMPLE_TOPIC_NAME = "pizza-topic";

    public static void main(String[] args) {
        Properties properties = new Properties();

        // 필수 config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDRESS_LOCAL);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek_v001");
        // 60 초
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "true");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        TopicPartition topicPartition = new TopicPartition(SIMPLE_TOPIC_NAME, 1);
        kafkaConsumer.assign(List.of(topicPartition));
        kafkaConsumer.seek(topicPartition, 2L);

        // main thread 참조 변수
        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도로 thread 로 kafka consumer wakeup() 메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("main program starts to exit by calling wakeup");
            // wakeup 은 컨슈머를 깨우는 것이다. 그리고 Long poll 을 abort 시킨다.
//            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

//        pollAutoCommitSync(kafkaConsumer);
//        pollAutoCommitAsync(kafkaConsumer);
        pollNoCommit(kafkaConsumer);
    }

    private static void pollAutoCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
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

                // async는 오류가 발생했을 때 callback 방식으로 알려준다.
                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        // 오류가 나면 offsets 정보에 담겨서 온다.
                        if (exception != null) {
                            LOGGER.error("offsets {} is not completed, error{}", offsets, exception);
                        }
                    }
                });
            }
        } catch (WakeupException e) {
            LOGGER.error("wakeup exception has been called");
        } finally {
            try {
                LOGGER.info("#### commit sync ");
                kafkaConsumer.commitSync();
            } catch (CommitFailedException e) {
                e.printStackTrace();
            } finally {
                LOGGER.info("finally consumer is closing");
                kafkaConsumer.close();
            }
        }
    }

    private static void pollAutoCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
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
                // sync 수동 커밋
                // poll 은 batch 단위로 실행되기 때문애 해당 batch 가 끝난 이후에 커밋하는게 좋다. (단일 record에 대해서도 할 수 있지만 성능 문제로 잘 쓰지 않는다)
                try {
                    if (consumerRecords.count() > 0) {
                        kafkaConsumer.commitSync();
                        LOGGER.info("commit sync has been called");
                    }

                    // 커밋을 여러번 시도(실패할 때 retry) 해서 전부 실패했을 때 CommitFailedException 던진다.
                } catch (CommitFailedException e) {
                    LOGGER.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            LOGGER.error("wakeup exception has been called");
        } finally {
            LOGGER.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    private static void pollNoCommit(KafkaConsumer<String, String> kafkaConsumer) {
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
            }
        } catch (WakeupException e) {
            LOGGER.error("wakeup exception has been called");
        } finally {
            LOGGER.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }
}
