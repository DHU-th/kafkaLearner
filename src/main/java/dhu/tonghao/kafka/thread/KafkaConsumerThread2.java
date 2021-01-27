package dhu.tonghao.kafka.thread;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaConsumerThread2 extends Thread {
    private KafkaConsumer<String, String> consumer;
    private ExecutorService executorService;
    private Map<TopicPartition, OffsetAndMetadata> offsets;

    public KafkaConsumerThread2(Properties properties, String topic, int threadNum) {
        this.consumer = new KafkaConsumer<>(properties);
        this.consumer.subscribe(Collections.singletonList(topic));
        this.executorService = new ThreadPoolExecutor(threadNum, threadNum, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(2000), new ThreadPoolExecutor.CallerRunsPolicy());
        this.offsets = new HashMap<>();
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    executorService.submit(() -> {
                        for (TopicPartition topicPartition : records.partitions()) {
                            List<ConsumerRecord<String, String>> recordList = records.records(topicPartition);
                            //TODO 处理recordList
                            long lastConsumedOffset = recordList.get(recordList.size() - 1).offset();
                            synchronized (offsets) {
                                if (!offsets.containsKey(topicPartition)) {
                                    offsets.put(topicPartition, new OffsetAndMetadata(lastConsumedOffset + 1));
                                } else {
                                    long position = offsets.get(topicPartition).offset();
                                    if (position < lastConsumedOffset + 1) {
                                        offsets.put(topicPartition, new OffsetAndMetadata(lastConsumedOffset + 1));
                                    }
                                }
                            }
                        }
                    });
                    synchronized (offsets) {
                        if (!offsets.isEmpty()) {
                            consumer.commitSync(offsets);
                            offsets.clear();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
