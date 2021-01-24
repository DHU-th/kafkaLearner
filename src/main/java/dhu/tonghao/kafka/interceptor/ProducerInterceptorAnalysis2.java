package dhu.tonghao.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author TongHao on 2021/1/9
 */
public class ProducerInterceptorAnalysis2 implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "th-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), modifiedValue,
                record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        //TODO
    }

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public void close() {
        System.out.println("[INFO] 拦截器B CLOSED");
    }
}
