package vn.vnpay.kafka;


import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

@Getter
@Setter
public class KafkaConsumerPool extends ObjectPool<KafkaConsumerCell> {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerPool.class);
    private static KafkaConsumerPool instance;
    private KafkaConfig kafkaConfig;
    private static final AtomicReference<LinkedBlockingQueue<String>> recordQueue = new AtomicReference<>(new LinkedBlockingQueue<>());

    public static final class SingletonHolder {
        private static final KafkaConsumerPool INSTANCE = new KafkaConsumerPool();
    }

    public static KafkaConsumerPool getInstance() {
        return KafkaConsumerPool.SingletonHolder.INSTANCE;
    }

    public void init(){
        log.info("Initialize Kafka consumer connection pool........................ ");
        setExpirationTime(kafkaConfig.getKafkaConnectionTimeout());
        setMaxPoolSize(kafkaConfig.getMaxPoolSize());
    }

    public String getRecord() throws Exception {
        log.info("Get Kafka Consumer pool record.......");
        return recordQueue.get().take();
    }
    public void createConsumerPolling() throws Exception {
        AtomicReference<KafkaConsumerCell> consumerCell = new AtomicReference<>();
        try {
            consumerCell.set(getMember());
        } catch (InterruptedException e) {
            throw new Exception("Consumer fail polling ", e);
        }

        log.info("Consumer {} start polling", consumerCell.get().getConsumer().groupMetadata().groupInstanceId());

        new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> recordsKafka = consumerCell.get().poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> r : recordsKafka) {
                        log.info("----");
                        log.info("Kafka consumer id {} receive data: partition = {}, offset = {}, key = {}, value = {}",
                                consumerCell.get().getConsumer().groupMetadata().groupInstanceId(),
                                r.partition(),
                                r.offset(), r.key(), r.value());

                        recordQueue.get().add(r.value());
                    }

                    try {
                        consumerCell.get().getConsumer().commitSync();
                    } catch (CommitFailedException e) {
                        log.info("Kafka commit has ex", e);
                        log.error("Commit failed", e);
                    }
                }
            }
            catch (Exception e){
                log.info("Kafka consumer has ex ", e);
                log.error("Kafka not start polling ", e);
            }
            finally {
                release(consumerCell.get());
            }
        }).start();
    }

    @Override
    protected KafkaConsumerCell create() {
        return KafkaConsumerCell.getInstance();
    }

    @Override
    public boolean isOpen(KafkaConsumerCell o) {
        return (!o.isClosed());
    }

    @Override
    public void close(KafkaConsumerCell o) {
        o.close();
    }
}
