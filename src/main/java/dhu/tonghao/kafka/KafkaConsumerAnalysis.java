package dhu.tonghao.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author TongHao on 2021/1/7
 */
public class KafkaConsumerAnalysis {
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
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = KafkaConsumerAnalysis.initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        /**
         * 订阅主题,直接订阅只支持一个，第二次会覆盖。使用正则表达式可以匹配多个
         * consumer.subscribe(Collections.singletonList(topic));
         * consumer.subscribe(Pattern.compile("kafka_demo.*"));
         */
        /**
         * 同时指定多个主题下的多个分区，以列表的形式 consumer.assign(Collections.singletonList(new
         * TopicPartition(topic, 0)));
         */
        // 获取该主题下的分区信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        partitionInfos.forEach(e -> topicPartitionList.add(new TopicPartition(e.topic(), e.partition())));
        consumer.assign(topicPartitionList);
        try {
            for (int i = 0; i < 100; i++) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                /* 按照主题-分区进行消费 */
                for (TopicPartition topicPartition : consumerRecords.partitions()) {
                    for (ConsumerRecord<String, String> record : consumerRecords.records(topicPartition)) {
                        System.out.println("分区信息：" + record.partition() + " 消息信息：" + record.value());
                    }
                }
            }
            //取消订阅
            consumer.unsubscribe();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
