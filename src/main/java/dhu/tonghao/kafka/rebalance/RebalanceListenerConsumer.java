package dhu.tonghao.kafka.rebalance;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import dhu.tonghao.kafka.KafkaConsumerAnalysis;

public class RebalanceListenerConsumer {
    public static void main(String[] args) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(KafkaConsumerAnalysis.initConfig());
        consumer.subscribe(Collections.singletonList(KafkaConsumerAnalysis.topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(offsets);
                offsets.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                //ToDO
            }
        });
        try {
            while (true) {
                ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : poll) {
                    // process the record
                    offsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                            new OffsetAndMetadata(consumerRecord.offset() + 1));
                }
                consumer.commitAsync(offsets, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
