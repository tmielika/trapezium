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
 *
 * setup HADR_2 topic with 4 partitions to use this sample
 */
public class Producer {

    private org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;

    private static final String topic = "HADR_2";

    //    private static final String SERVER_ADDRESS = "md-bdadev-55.verizon.com:9092";
    private static final String SERVER_ADDRESS = "localhost:9092";

    private static final String RECORD_1 = "AA1.IAD8.ALTER.NET|em0|108|2015-07-07 15:52:43|299990|11|" ;
    private static final String RECORD_2 = "BB1.GOOG.ALTER.COM|em0|101|2015-07-07 16:41:21|300000|66|" ;
    private static final String RECORD_3 = "CC1.GOOG.ALTER.COM|em0|101|2015-07-07 16:41:21|300000|77|" ;
    private static final String RECORD_4 = "DD1.GOOG.ALTER.COM|em0|101|2015-07-07 16:41:21|300000|99|" ;
    private static final String RECORD_5 = "ZZ1.GOOG.ALTER.COM|em0|101|2015-07-07 16:41:21|300000|00|" ;

    static int PARTITIONS = 2;

    public void initialize() {

        Map<String, Object> producerProps = new HashMap<String,Object>();
        producerProps.put("bootstrap.servers", SERVER_ADDRESS);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("request.required.acks", "1");
        producerProps.put("partitioner.class", Partitioner.class.getName());
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps);
    }

    public void publishMesssage() throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Start sending messages: ");

        long count = 0;

        boolean spin = true;

        while (spin) {

            count++;

            producer.send(new ProducerRecord<String, String>(topic, "1", RECORD_1+count ));

            producer.send(new ProducerRecord<String, String>(topic, "2", RECORD_2+count ));

            producer.send(new ProducerRecord<String, String>(topic, "3", RECORD_3+count ));

            producer.send(new ProducerRecord<String, String>(topic, "4", RECORD_4+count ));

            producer.send(new ProducerRecord<String, String>(topic, "5", RECORD_5+count ));

            if (count % 10 == 0) {
                System.out.println("Sleeping for a while ... count= " + count );
                Thread.sleep(10*1000);
            }
        }


        producer.close();
    }

    public static void main(String[] args) throws Exception {
        Producer kafkaProducer = new Producer();
        // Initialize producer
        kafkaProducer.initialize();
        // Publish message
        kafkaProducer.publishMesssage();
        //Close the producer

    }


    public static class Partitioner implements org.apache.kafka.clients.producer.Partitioner {


        public int partition(Object key, int a_numPartitions) {
            int partition = 0;
            String stringKey = (String) key.toString();
            int offset = Integer.parseInt(stringKey);
            return offset % a_numPartitions;
        }

        public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
            return partition(o, 5);
        }

        public void close() {

        }

        public void configure(Map<String, ?> map) {

        }
    }
}
