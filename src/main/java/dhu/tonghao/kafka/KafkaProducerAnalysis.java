package dhu.tonghao.kafka;

import dhu.tonghao.kafka.interceptor.ProducerInterceptorAnalysis;
import dhu.tonghao.kafka.interceptor.ProducerInterceptorAnalysis2;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
/**
 * @author TongHao on 2021/1/7
 */
public class KafkaProducerAnalysis {

    /** 有多个可以用逗号隔开 */
    public static final String brokerList = "47.93.121.123:9092";
    public static final String topic = "kafka_demo_analysis";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "0");
        properties.put(ProducerConfig.RETRIES_CONFIG, 10); //重试次数
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ProducerInterceptorAnalysis.class.getName() + "," + ProducerInterceptorAnalysis2.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = KafkaProducerAnalysis.initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Scanner scanner = new Scanner(System.in);
        for (int i = 0; i < 5; i++) {
            try {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, scanner.nextLine());
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            //TODO
                            exception.printStackTrace();
                        } else {
                            System.out.println(metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset());

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
