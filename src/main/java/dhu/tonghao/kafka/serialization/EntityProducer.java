package dhu.tonghao.kafka.serialization;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class EntityProducer {
    /** 有多个可以用逗号隔开 */
    public static final String brokerList = "47.93.121.123:9092";
    public static final String topic = "kafka_serialization_analysis";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EntitySerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "0");
        properties.put(ProducerConfig.RETRIES_CONFIG, 10); //重试次数
        //        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        //                ProducerInterceptorAnalysis.class.getName() + "," + ProducerInterceptorAnalysis2.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        KafkaProducer<String, Entity> producer = new KafkaProducer<String, Entity>(EntityProducer.initConfig());
        Scanner scanner = new Scanner(System.in);
        for (int i = 0; i < 5; i++) {
            try {
                Entity value = new Entity(scanner.nextLine(), scanner.nextInt());
                scanner.nextLine();
                ProducerRecord<String, Entity> producerRecord = new ProducerRecord<>(topic, scanner.nextLine(), value);
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
