package dhu.tonghao.kafka.serialization;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.alibaba.fastjson.JSON;

public class EntityConsumer {

    public static final String brokerList = "47.93.121.123:9092";
    public static final String topic = "kafka_serialization_analysis";
    public static final String groupId = "kafka-learner";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EntityDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); //消费组
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "0");
        return properties;
    }

    public static void main(String[] args) {
        //        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 1000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000),
        //                new ThreadPoolExecutor.CallerRunsPolicy());
        KafkaConsumer<String, Entity> consumer = new KafkaConsumer<>(EntityConsumer.initConfig());
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                ConsumerRecords<String, Entity> poll = consumer.poll(Duration.ofMillis(1000));
                poll.forEach(e -> {
                    System.out.println(e.offset() + "_" + e.key() + "_" + JSON.toJSONString(e.value()));
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
