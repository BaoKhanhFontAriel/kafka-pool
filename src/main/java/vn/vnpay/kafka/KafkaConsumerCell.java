package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


@Slf4j
@Getter
@Setter
public class KafkaConsumerCell {
    private boolean closed;
    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;

    public static final class SingletonHolder {
        private static final KafkaConsumerCell INSTANCE = new KafkaConsumerCell();
    }

    public static KafkaConsumerCell getInstance() {
        return KafkaConsumerCell.SingletonHolder.INSTANCE;
    }
    public KafkaConsumerCell() {
        KafkaConfig kafkaConfig = KafkaConsumerPool.getInstance().getKafkaConfig();
        String consumerTopic = kafkaConfig.getKafkaConsumerTopic();
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaServer());
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getKafkaConsumerGroupId());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Collections.singletonList(consumerTopic));
        log.info("create consumer {} - partition {} - topic {}", consumer.groupMetadata().groupInstanceId(), consumer.assignment(), consumerTopic);
    }
    public void close() {
        try {
            consumer.close();
            closed = true;
        } catch (Exception e) {
            log.error("connection can not closed: {0}", e);
        }
    }

    public ConsumerRecords<String, String> poll(Duration ofMillis) {
        return consumer.poll(ofMillis);
    }
}
