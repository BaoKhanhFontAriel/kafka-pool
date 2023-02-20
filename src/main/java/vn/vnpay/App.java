package vn.vnpay;

import vn.vnpay.kafka.KafkaConsumerPool;
import vn.vnpay.kafka.KafkaProducerCell;
import vn.vnpay.kafka.KafkaProducerPool;
import vn.vnpay.util.ExecutorSingleton;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        // start kafka pool
        ExecutorSingleton.getInstance();
        KafkaProducerPool.getInstancePool().init();
        KafkaConsumerPool.getInstancePool().init();
        KafkaConsumerPool.getInstancePool().startPoolPolling();

        // shutdown kafka pool
        KafkaProducerPool.getInstancePool().shutdown();
        KafkaConsumerPool.getInstancePool().shutdown();
    }
}
