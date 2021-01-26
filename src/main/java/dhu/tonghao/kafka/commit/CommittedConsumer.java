package dhu.tonghao.kafka.commit;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.alibaba.fastjson.JSON;

public class CommittedConsumer {
    /** 有多个可以用逗号隔开 */
    public static final String brokerList = "47.93.121.123:9092";
    public static final String topic = "kafka_demo_analysis";
    /** 消费组的名称 */
    public static final String groupId = "kafka-learner";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); //消费组
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "0");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return properties;
    }

    public static void main(String[] args) {
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(CommittedConsumer.initConfig());
        consumer.assign(Collections.singletonList(topicPartition));
        long lastConsumedOffset = -1;//当前消费到的位移
        for (int i = 0; i < 50; i++) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
            if (poll.isEmpty()) {
                continue;
            }
            //只订阅了一个tp,所以只要消费这一个
            List<ConsumerRecord<String, String>> records = poll.records(topicPartition);
            for (ConsumerRecord<String, String> record : records) {
                //TODO
            }
            lastConsumedOffset = records.get(records.size() - 1).offset();
            consumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastConsumedOffset)),
                    new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            if (e == null) {
                                System.out.println("消费成功! offset:" + JSON.toJSONString(map));
                            } else {
                                //TODO
                            }
                        }
                    });
        }
        long position = consumer.position(topicPartition);
        OffsetAndMetadata committed = consumer.committed(topicPartition);
        long committedOffset = committed.offset();
        System.out.println("lastConsumedOffset: " + lastConsumedOffset + " committedOffset: " + committedOffset
                + " position: " + position);
    }
}
