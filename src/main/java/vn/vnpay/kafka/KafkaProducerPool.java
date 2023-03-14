package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


@Getter
@Setter
@Slf4j
public class KafkaProducerPool extends ObjectPool<KafkaProducer<String, String>> {
    private static KafkaProducerPool instance;
    private KafkaConfig kafkaConfig;



    private static final class SingletonHolder {
        private static final KafkaProducerPool INSTANCE = new KafkaProducerPool();
    }

    public static KafkaProducerPool getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public void init() {
        log.info("Initialize Kafka Producer Connection pool........................ ");
        setExpirationTime(kafkaConfig.getKafkaConnectionTimeout());
        setMaxPoolSize(kafkaConfig.getMaxPoolSize());
    }

    public void send(String message) throws Exception {
        log.info("Kafka send.........");
        KafkaProducer<String, String> producer = getMember();

        // send message
        log.info("Message send {}", message);
        ProducerRecord<String, String> recordKafka =
                new ProducerRecord<>(kafkaConfig.getKafkaProducerTopic(), message);
        try {
            producer.send(recordKafka, (recordMetadata, e) -> {
                if (e == null) {
                    log.info("Kafka producer successfully send recordKafka as: Topic = {}, partition = {}, Offset = {}",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());

                } else {
                    log.info("Kafka producer has ex", e);
                    log.error("Can't produce,getting error", e);
                }
            });
        } catch (Exception e) {
            throw new Exception("Kafka can not produce message ", e);
        } finally {
            release(producer);
        }
    }

    @Override
    protected KafkaProducer<String, String> create() {
        return KafkaProducerBean.getInstance().openConnection();
    }

    @Override
    public boolean isOpen(KafkaProducer<String, String> o) {
        boolean isOpen = true;
        try {
            o.partitionsFor(kafkaConfig.getKafkaProducerTopic());
        }
        catch (Exception e){
            log.error("Kafka producer has closed ", e);
            isOpen = false;
        }
        return isOpen;
    }

    @Override
    public void close(KafkaProducer<String, String> o) {
        o.close();
    }
}
