package dhu.tonghao.kafka.serialization;

import com.alibaba.fastjson.JSON;

public class Entity {
    private String name;
    private Integer age;

    public Entity(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Entity() {
    }

    public static void main(String[] args) {
        byte[] bytes = JSON.toJSONString(new Entity("主题", 10)).getBytes();
        Entity entity = JSON.parseObject(new String(bytes), Entity.class);
        System.out.println(entity);
    }
}
