package dhu.tonghao.kafka.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.alibaba.fastjson.JSON;

public class EntityDeserializer implements Deserializer<Entity> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Entity deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return JSON.parseObject(new String(data), Entity.class);
    }

    @Override
    public void close() {

    }
}
