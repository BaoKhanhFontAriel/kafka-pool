package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
@Getter
@Setter
public class KafkaProducerCell {
    private long relaxTime;
    private long timeOut;
    private boolean isClosed;
    private org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    private String producerTopic;

//    private static final class SingletonHolder {
//        private static final KafkaProducerCell INSTANCE = new KafkaProducerCell();
//    }
//
//    public static KafkaProducerCell getInstance() {
//        return KafkaProducerCell.SingletonHolder.INSTANCE;
//    }
    public KafkaProducerCell() {
        KafkaConfig kafkaConfig = KafkaProducerPool.getInstance().getKafkaConfig();
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaServer());
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);
        log.info("Create new producer {}", producer.metrics());
    }
    public void close() {
        try {
            producer.close();
            isClosed = true;
        } catch (Exception e) {
            log.warn("connection is closed: {0}", e);
        }
    }
}
