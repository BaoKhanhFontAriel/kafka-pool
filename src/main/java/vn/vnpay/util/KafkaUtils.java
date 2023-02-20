package vn.vnpay.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import vn.vnpay.kafka.*;

import java.util.*;

@Slf4j
public class KafkaUtils {
    public static String sendAndReceive(String data) throws Exception {
        log.info("send and receive: {}", data);
        send(data);
        String res = receive();
        log.info("response is: {}", res);
        return res;
    }

    public static String receive() throws Exception {
        log.info("Kafka start receiving.........");
        return KafkaConsumerPool.getRecord();
    }

    public static void send(String message) throws Exception {
        log.info("Kafka send.........");
        KafkaProducerCell producerCell = KafkaProducerPool.getInstancePool().getConnection();
        KafkaProducer<String, String> producer = producerCell.getProducer();

        // send message
        log.info("message send {}", message);
        ProducerRecord<String, String> record = new ProducerRecord<>(producerCell.getProducerTopic(), message);
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

        KafkaProducerPool.getInstancePool().releaseConnection(producerCell);
    }

    private static AdminClient adminClient;

    public static void createNewTopic(String topic, int partition, short replica) {
        //        //create partition
        if (adminClient == null){
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:29092");
            adminClient = AdminClient.create(props);
        }

        NewTopic newTopic = new NewTopic(topic, partition, replica);
        adminClient.createTopics(Arrays.asList(newTopic));

        adminClient.close();
    }
}
