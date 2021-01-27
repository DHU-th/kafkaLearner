package dhu.tonghao.kafka.thread;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerThread1 extends Thread {
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerThread1(Properties properties, String topic) {
        this.consumer = new KafkaConsumer<String, String>(properties);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    //TODO
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
