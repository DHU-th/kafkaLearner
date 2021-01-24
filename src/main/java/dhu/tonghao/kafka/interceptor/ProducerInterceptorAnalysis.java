package dhu.tonghao.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author TongHao on 2021/1/9
 */
public class ProducerInterceptorAnalysis implements ProducerInterceptor<String, String> {

    private volatile long success = 0L;
    private volatile long fail = 0L;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "dhu-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), modifiedValue,
                record.headers());
    }

    @Override
    @SuppressWarnings("all")
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            success++;
        } else {
            fail++;
        }
    }

    @Override
    public void close() {
        System.out.println("[INFO] 拦截器A CLOSED， 成功: " + success + "次, 失败：" + fail + "次！");
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}