package vn.vnpay.kafka;

import lombok.*;


@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaConfig {
    private int initProducerPoolSize;
    private int initConsumerPoolSize;
    private long kafkaConnectionTimeout;
    private String kafkaConsumerTopic;
    private String kafkaConsumerGroupId;
    private String kafkaProducerTopic;
    private String kafkaServer;

}
