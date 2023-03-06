package vn.vnpay.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import vn.vnpay.kafka.*;

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
        KafkaProducerPool.send(message);
    }
    public static void startPoolPolling() {
        log.info("Start Kafka consumer pool polling.........");
        int count = 10;
        while (count > 0) {
            ExecutorSingleton.getInstance().getExecutorService().submit((Runnable) KafkaConsumerPool::createPolling);
            count--;
        }
    }


//    private static AdminClient adminClient;
//
//    public static void createNewTopic(String topic, int partition, short replica) {
//        //        //create partition
//        if (adminClient == null){
//            Properties props = new Properties();
//            props.put("bootstrap.servers", "localhost:29092");
//            adminClient = AdminClient.create(props);
//        }
//
//        NewTopic newTopic = new NewTopic(topic, partition, replica);
//        adminClient.createTopics(Arrays.asList(newTopic));
//
//        adminClient.close();
//    }
}
