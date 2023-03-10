package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Getter
@Setter
public class KafkaProducerPool extends ObjectPool<KafkaProducerCell> {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerPool.class);
    private static KafkaProducerPool instance;
    private KafkaConfig kafkaConfig;

    private static final class SingletonHolder {
        private static final KafkaProducerPool INSTANCE = new KafkaProducerPool();
    }

    public static KafkaProducerPool getInstance() {
        return KafkaProducerPool.SingletonHolder.INSTANCE;
    }
    
    public void init(){
        log.info("Initialize Kafka Producer Connection pool........................ ");
        setExpirationTime(kafkaConfig.getKafkaConnectionTimeout());
        setMaxPoolSize(kafkaConfig.getMaxPoolSize());
    }

    public void send(String message) throws Exception {
        log.info("Kafka send.........");
        KafkaProducerCell producerCell = getMember();
        KafkaProducer<String, String> producer = producerCell.getProducer();

        // send message
        log.info("Message send {}", message);
        ProducerRecord<String, String> recordKafka =
                new ProducerRecord<>(kafkaConfig.getKafkaProducerTopic(), message);
        try{
            producer.send(recordKafka, (recordMetadata, e) -> {
                if (e == null) {
                    log.info("Kafka producer successfully send recordKafka as: Topic = {}, partition = {}, Offset = {}",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());

                } else {
                    log.info("Kafka producer has ex", e);
                    log.error("Can't produce,getting error", e);
                }
            });
        }
        catch (Exception e){
            throw new Exception("Kafka can not produce message ", e);
        }
        finally {
            release(producerCell);
        }
    }
    @Override
    protected KafkaProducerCell create() {
        return new KafkaProducerCell();
    }

    @Override
    public boolean isOpen(KafkaProducerCell o) {
        return (!o.isClosed());
    }

    @Override
    public void close(KafkaProducerCell o) {
        o.close();
    }
}
