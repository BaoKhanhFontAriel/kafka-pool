package vn.vnpay.kafka;

import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;
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
public class KafkaProducerBean {
    private long relaxTime;
    private boolean isClosed;
    private KafkaProducer<String, String> producer;

    private static final class SingletonHolder {
        private static final KafkaProducerBean INSTANCE = new KafkaProducerBean();
    }

    public static KafkaProducerBean getInstance() {
        return SingletonHolder.INSTANCE;
    }
    public KafkaProducer<String, String> openConnection(){
        KafkaConfig kafkaConfig = KafkaProducerPool.getInstance().getKafkaConfig();
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaServer());
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);
        log.info("Create new producer {}", producer);
        return producer;
    }

}
