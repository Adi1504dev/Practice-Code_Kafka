import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Lesson_a0_SimpleProducerClass {
    public static void main(String args[]) {
        //Set producer config
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //Here we are telling data producer will be producing string type of data
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Key and value serializable properties let kafka know what type of value producer will be sending
        // and how it can be serialized

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> p = new ProducerRecord<String, String>("first_topic", "Hello I am java Producer");

        //Send data
        //the below statement is asynchronous so just send wont work until either you flush the data
        // or close the producer
        producer.send(p);
        //any one of below is required to send the data

        producer.flush();
    }
}
