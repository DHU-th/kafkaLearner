package dhu.tonghao.kafka.partitioner;

import dhu.tonghao.kafka.KafkaProducerAnalysis;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author TongHao on 2021/1/7
 */
public class Partitioner {

    public static void main(String[] args) {
        Properties properties = KafkaProducerAnalysis.initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //建议按照这个顺序调试，更容易看出结果
        ProducerRecord<String, String> record1 = new ProducerRecord<>(KafkaProducerAnalysis.topic, 0, "我是key", "指定分区");
        ProducerRecord<String, String> record2 = new ProducerRecord<>(KafkaProducerAnalysis.topic, "我是key", "带有value");
        ProducerRecord<String, String> record3 = new ProducerRecord<>(KafkaProducerAnalysis.topic, "只有value");

        producer.send(record1);
        producer.send(record2);
        producer.send(record3);
        producer.close();
    }
}