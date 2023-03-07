package vn.vnpay.kafka;


import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

@Getter
@Setter
public class KafkaConsumerPool extends ObjectPool<KafkaConsumerCell> {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerPool.class);
    private static KafkaConsumerPool instance;
    private KafkaConfig kafkaConfig;
    private static final AtomicReference<LinkedBlockingQueue<String>> recordQueue = new AtomicReference<>(new LinkedBlockingQueue<>());

    public static synchronized KafkaConsumerPool getInstance() {
        if (instance == null) {
            instance = new KafkaConsumerPool();
        }
        return instance;
    }

    public void init(){
        log.info("Initialize Kafka consumer connection pool........................ ");
        setExpirationTime(kafkaConfig.getKafkaConnectionTimeout());
    }

    public String getRecord() throws Exception {
        log.info("Get Kafka Consumer pool record.......");
        return recordQueue.get().take();
    }
    public void createConsumerPolling() throws Exception {
        KafkaConsumerCell consumerCell = null;
        try {
            consumerCell = getConnection();
        } catch (InterruptedException e) {
            throw new Exception("consumer fail polling ", e);
        }

        log.info("consumer {} start polling", consumerCell.getConsumer().groupMetadata().groupInstanceId());

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumerCell.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> r : records) {
                    log.info("----");
                    log.info("kafka consumer id {} receive data: partition = {}, offset = {}, key = {}, value = {}",
                            consumerCell.getConsumer().groupMetadata().groupInstanceId(),
                            r.partition(),
                            r.offset(), r.key(), r.value());

                    recordQueue.get().add(r.value());
                }

                try {
                    consumerCell.getConsumer().commitSync();
                } catch (CommitFailedException e) {
                    log.error("commit failed", e);
                }
            }
        }
        catch (Exception e){
            throw new Exception("Kafka not start polling ", e);
        }
        finally {
            KafkaConsumerPool.getInstance().releaseConnection(consumerCell);
        }
    }

    public KafkaConsumerCell getConnection() throws InterruptedException {
        return super.checkOut();
    }

    public void releaseConnection(KafkaConsumerCell kafkaConsumerCell){
        super.checkIn(kafkaConsumerCell);
    }

    @Override
    protected KafkaConsumerCell create() {
        return KafkaConsumerCell.getInstance();
    }

    @Override
    public boolean validate(KafkaConsumerCell o) {
        return (!o.isClosed());
    }

    @Override
    public void expire(KafkaConsumerCell o) {
        o.close();
    }
}
