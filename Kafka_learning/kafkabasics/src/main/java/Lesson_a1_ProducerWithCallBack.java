import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Lesson_a1_ProducerWithCallBack {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Lesson_a1_ProducerWithCallBack.class);
        //Set producer Config
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        //Producer is super class of kafkaProducer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
     for(int i=0;i<50;i++)
     {
        ProducerRecord<String, String> pr = new ProducerRecord<String, String>("first_topic", "Hello I am Producer with call back"+i);

        //Send data
        producer.send(pr, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("data Written to topic : " + recordMetadata.topic());
                    logger.info("data written to Partition : " + recordMetadata.partition());
                    logger.info("data written to offset : " + recordMetadata.offset());
                    logger.info("data written at : " + recordMetadata.timestamp());
                } else
                    logger.error("Error while producing records : " + e);
                ;
            }
        });
        producer.flush();
    }

    }
}