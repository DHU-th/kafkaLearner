package dhu.tonghao.kafka.thread;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class MultiProducer {
    /** 有多个可以用逗号隔开 */
    public static final String brokerList = "47.93.121.123:9092,47.93.121.123:9093,47.93.121.123:9094";
    public static final String topic = "server-cluster";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "0");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 10); //重试次数
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = MultiProducer.initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Scanner scanner = new Scanner(System.in);
        for (int i = 0; i < 5; i++) {
            try {
                String key = scanner.nextLine();
                String value = scanner.nextLine();
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            //TODO
                            exception.printStackTrace();
                        } else {
                            System.out.println(
                                    metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset() + "发送成功！");

                        }
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
