package vn.vnpay.kafka;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaConfig {

    private static final class SingletonHolder {
        private static final KafkaConfig INSTANCE = new KafkaConfig();
    }

    public static KafkaConfig getInstance() {
        return KafkaConfig.SingletonHolder.INSTANCE;
    }
    private int initProducerPoolSize;
    private int initConsumerPoolSize;
    private long kafkaConnectionTimeout;
    private String kafkaConsumerTopic;
    private String kafkaConsumerGroupId;
    private String kafkaProducerTopic;
    private String kafkaServer;
//    public static final int INIT_PRODUCER_POOL_SIZE = AppConfigSingleton.getInstance().getIntProperty("kafka.init_producer_pool_size");
//    public static final int INIT_CONSUMER_POOL_SIZE = AppConfigSingleton.getInstance().getIntProperty("kafka.init_consumer_pool_size");
//    public static final long TIME_OUT = AppConfigSingleton.getInstance().getIntProperty("kafka.timeout");;
//    public static final String KAFKA_CONSUMER_TOPIC = AppConfigSingleton.getInstance().getStringProperty("kafka.topic.consumer");
//    public static final String KAFKA_CONSUMER_GROUP_ID = AppConfigSingleton.getInstance().getStringProperty("kafka.consumer.group_id");
//    public static final String KAFKA_PRODUCER_TOPIC = AppConfigSingleton.getInstance().getStringProperty("kafka.topic.producer");
//    public static final String KAFKA_SERVER = AppConfigSingleton.getInstance().getStringProperty("kafka.server");
}
