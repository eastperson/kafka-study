package org.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class.getName());
    private StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();
    private String specialKeyName;

    // 인자는 Producer Properties
    @Override
    public void configure(Map<String, ?> properties) {
        this.specialKeyName = properties.get("custom.specialKey").toString();
    }

    // cluster 인자는 브로커의 가지고 있는 정보를 가져올 수 있다.
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);

        int partitionsSize = partitionInfos.size();
        int specialPartitions = (int) (partitionsSize * 0.5);
        int partitionIndex;

        if (keyBytes == null) {
            // throw new InvalidRecordException("key should not be null");
            return stickyPartitionCache.partition(topic, cluster);
        }

        if (this.specialKeyName.equals(key.toString())) {
            // key bytes 로 하면 고정값이 되버리니깐 valueBytes를 사용한다.
            // 1, 2 가 나와야 한다.
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % specialPartitions;
        } else {
            // 2, 3, 4 가 나와야 한다.
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (partitionsSize - specialPartitions) + specialPartitions;
        }
        logger.info("key:{} is sent to partition:{}", key, partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }
}
