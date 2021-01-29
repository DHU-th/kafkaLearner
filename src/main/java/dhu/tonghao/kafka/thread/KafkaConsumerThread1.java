package dhu.tonghao.kafka.thread;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaConsumerThread1 extends Thread {
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerThread1(Properties properties, String topic) {
        this.consumer = new KafkaConsumer<String, String>(properties);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            //这里要先poll到有效值才能获取到这个消费者拥有的分区
            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.isEmpty()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(
                            "分区是：" + record.partition() + " 分区的key是：" + record.key() + " 分区的值是：" + record.value());
                }
                assignment = consumer.assignment();//获取订阅的分区
            }
            String name = "consumer_" + Thread.currentThread().getName();
            StringBuilder partitions = new StringBuilder();
            consumer.assignment().forEach(e -> partitions.append(e.partition()).append("区 "));
            System.out.println(name + "被分到的分区有：" + partitions.toString());
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(
                            "分区是：" + record.partition() + " 分区的key是：" + record.key() + " 分区的值是：" + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
