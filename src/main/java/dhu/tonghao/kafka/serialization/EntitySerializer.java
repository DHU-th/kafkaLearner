package dhu.tonghao.kafka.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.alibaba.fastjson.JSON;

public class EntitySerializer implements Serializer<Entity> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, Entity data) {
        if (data == null) {
            return null;
        }
        return JSON.toJSONString(data).getBytes();
    }

    @Override
    public void close() {

    }
}
