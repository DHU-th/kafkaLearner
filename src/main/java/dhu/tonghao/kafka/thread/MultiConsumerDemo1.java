package dhu.tonghao.kafka.thread;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MultiConsumerDemo1 {
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
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(MultiConsumerDemo1.initConfig());
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        final int consumerThreadNum = partitionInfos.size() / 2; //注意不要超过分区的数目, 这里设置成[1，分区数]都可以
        consumer.close();
        for (int i = 0; i < consumerThreadNum; i++) {
            new KafkaConsumerThread1(properties, topic).start();
        }
    }
}
