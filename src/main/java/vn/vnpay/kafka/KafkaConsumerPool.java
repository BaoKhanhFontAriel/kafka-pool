package vn.vnpay.kafka;


import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

@Getter
@Setter
@Slf4j
public class KafkaConsumerPool extends ObjectPool<KafkaConsumer<String,String>> {
    private static KafkaConsumerPool instance;
    private KafkaConfig kafkaConfig;
    private static final AtomicReference<LinkedBlockingQueue<String>> recordQueue
            = new AtomicReference<>(new LinkedBlockingQueue<>());

    private static final class SingletonHolder {
        private static final KafkaConsumerPool INSTANCE = new KafkaConsumerPool();
    }

    public static KafkaConsumerPool getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public void init() {
        log.info("Initialize Kafka consumer connection pool........................ ");
        setExpirationTime(kafkaConfig.getKafkaConnectionTimeout());
        setMaxPoolSize(kafkaConfig.getMaxPoolSize());
    }

    public String getRecord() throws Exception {
        log.info("Get Kafka Consumer pool record.......");
        return recordQueue.get().take();
    }

    public void createConsumerPolling() throws Exception {
        AtomicReference<KafkaConsumer<String,String>> consumer = new AtomicReference<>();
        try {
            consumer.set(getMember());
        } catch (Exception e) {
            throw new Exception("Fail getting kafka consumer", e);
        }

        log.info("Consumer {} start polling", consumer.get().groupMetadata().groupInstanceId());

        new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> recordsKafka = consumer.get().poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> r : recordsKafka) {
                        log.info("----");
                        log.info("Kafka consumer id {} receive data: partition = {}, offset = {}, key = {}, value = {}",
                                consumer.get().groupMetadata().groupInstanceId(),
                                r.partition(),
                                r.offset(), r.key(), r.value());

                        recordQueue.get().add(r.value());
                    }
                    consumer.get().commitSync();

                }
            } catch (Exception e) {
                log.info("Kafka consumer has ex ", e);
                log.error("Kafka not start polling ", e);
            } finally {
                release(consumer.get());
            }
        }).start();
    }

    @Override
    protected KafkaConsumer<String,String> create() {
        return KafkaConsumerBean.getInstance().createConnection();
    }

    @Override
    public boolean isOpen(KafkaConsumer<String,String> o) {
        boolean isOpen = true;
        try {
            o.poll(Duration.ofMillis(1));
        }
        catch (Exception e){
            log.error("Kafka consumer has closed ", e);
            isOpen = false;
        }
        return isOpen;
    }

    @Override
    public void close(KafkaConsumer<String,String> o) {
        o.close();
    }
}
