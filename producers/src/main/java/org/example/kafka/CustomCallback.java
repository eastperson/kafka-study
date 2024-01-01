package org.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomCallback.class.getName());

    private final int seq;

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (exception == null) {
            LOGGER.info("seq:{} partition:{} offset:{}", seq, recordMetadata.partition(), recordMetadata.offset());
        } else {
            LOGGER.error("exception error from broker " + exception.getMessage());
        }
    }
}
