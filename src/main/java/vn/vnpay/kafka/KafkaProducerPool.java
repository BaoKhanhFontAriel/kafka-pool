package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


@Getter
@Setter
public class KafkaProducerPool extends ObjectPool<KafkaProducerCell> {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerPool.class);
    private static KafkaProducerPool instance;
    private Properties producerProps;
    private KafkaConfig kafkaConfig;

    public static synchronized KafkaProducerPool getInstance() {
        if (instance == null) {
            instance = new KafkaProducerPool();
        }
        return instance;
    }
    
    public void init(){
        log.info("Initialize Kafka Producer Connection pool........................ ");
        setExpirationTime(kafkaConfig.getKafkaConnectionTimeout());
        producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaServer());
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public void send(String message) throws Exception {
        log.info("Kafka send.........");
        KafkaProducerCell producerCell = getConnection();
        KafkaProducer<String, String> producer = producerCell.getProducer();

        // send message
        log.info("message send {}", message);
        ProducerRecord<String, String> record =
                new ProducerRecord<>(kafkaConfig.getKafkaProducerTopic(), message);
        try{
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    log.info("Kafka producer successfully send record as: Topic = {}, partition = {}, Offset = {}",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());

                } else {
                    log.error("Can't produce,getting error", e);
                }
            });
        }
        catch (Exception e){
            throw new Exception("Kafka can not produce message");
        }

        KafkaProducerPool.getInstance().releaseConnection(producerCell);
    }

    public synchronized KafkaProducerCell getConnection() {
        log.info("Get kafka production connection.............");
        return super.checkOut();
    }

    public void releaseConnection(KafkaProducerCell consumer) {
        log.info("begin releasing connection {}", consumer);
        super.checkIn(consumer);
    }

    @Override
    protected KafkaProducerCell create() {
        return (new KafkaProducerCell(producerProps));
    }

    @Override
    public boolean validate(KafkaProducerCell o) {
        return (!o.isClosed());
    }

    @Override
    public void expire(KafkaProducerCell o) {
        o.close();
    }
}
