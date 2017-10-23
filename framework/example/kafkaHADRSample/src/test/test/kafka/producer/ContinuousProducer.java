package test.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sankma8 on 8/7/17.
 * @deprecated this was just a test
 */
public class ContinuousProducer {


    public static class SimplePartitioner implements org.apache.kafka.clients.producer.Partitioner {

//        public SimplePartitioner (VerifiableProperties props) {
//
//        }

        public int partition(Object key, int a_numPartitions) {
//            System.out.println("PARTITIONS = "+a_numPartitions);
            int partition = 0;
            String stringKey = (String) key.toString();
            int offset = Integer.parseInt(stringKey);
            return offset % a_numPartitions;
        }

        public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
            return partition(o,2);
        }

        public void close() {

        }

        public void configure(Map<String, ?> map) {

        }
    }
    private static org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    private static final String topic= "SMAHESH";

    static int PARTITIONS = 2;

    public void initialize() {
        Map<String,Object> producerProps = new HashMap<String,Object>();
        producerProps.put("bootstrap.servers", "md-bdadev-55.verizon.com:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("request.required.acks", "1");
        producerProps.put("partitioner.class", SimplePartitioner.class);
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps);
    }
    public void publishMesssage() throws Exception{
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Start sending messages: ");

        int myKey  = 0;

        while (true){
            myKey ++;
            String msg = ""+myKey;
//            ProducerRecord
            //Define topic name and message
            producer.send(new ProducerRecord<String, String>(topic,
                    Integer.toString(myKey), Integer.toString(myKey))); // This publishes message on given topic
            if(myKey%10==0) {
                System.out.println("Sleeping for a while ...");
                Thread.sleep(1000);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ContinuousProducer kafkaProducer = new ContinuousProducer();
        // Initialize producer
        kafkaProducer.initialize();
        // Publish message
        kafkaProducer.publishMesssage();
        //Close the producer
        producer.close();
    }
}
