package dhu.tonghao.kafka.seek;

import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SeekConsumer {
    public static final String brokerList = "47.93.121.123:9092，47.93.121.123:9093，47.93.121.123:9094";
    public static final String topic = "kafka-seek-analysis";
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
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(SeekConsumer.initConfig());
        consumer.subscribe(Collections.singleton(topic));
        /**
         * 注意,此时消费者只是订阅了主题,但是还没有被分配到指定的分区 直接指定会报异常
         **/
        // 为每个分区设置的offset，需要根据业务来
        //consumer.seek(new TopicPartition(topic, 100), 10);
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.isEmpty()) {
            consumer.poll(Duration.ofMillis(1000));
            assignment = consumer.assignment();//获取订阅的分区
        }
        long fetchDateTime = new Date().getTime() - 2 * 60 * 60 * 1000;
        Map<TopicPartition, Long> timestampMap = new HashMap<>();
        /** 确保被分配了指定的分区,再用seek指定这些分区开始位移消费的位置 */
        for (TopicPartition tp : assignment) {
            // 比如说要从2个小时前的消息开始消费
            timestampMap.put(tp, fetchDateTime);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampMap);
        offsets.forEach((k, v) -> consumer.seek(k, v.offset()));
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
            //TODO
        }
    }
}
