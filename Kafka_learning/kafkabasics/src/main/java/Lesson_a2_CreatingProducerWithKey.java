import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class Lesson_a2_CreatingProducerWithKey {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(Lesson_a1_ProducerWithCallBack.class);
        //Creating producer Config
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        //Creating producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 5; i++) {

            ProducerRecord<String, String> pr = new ProducerRecord<String, String>("first_topic", "id_"+i, "I am Kafka Producer with Key"+i);
            logger.info("Key Id_"+i);//Key logging
            //id_0 ----->>>Partition 1
            //id_1 ----->>>Partition 0
            //id_2 ----->>>Partition 2
            //id_3 ----->>>Partition 0
            //id_4 ----->>>Partition 2
            //First re run and test the code if id is same partition will be same
            //Also keep id constant in one case like Id_1




            //Send Data--asynchronus
            producer.send(pr, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Data Written to topic : " + recordMetadata.topic());
                        logger.info("data written to Partition : " + recordMetadata.partition());
                        logger.info("data written to offset : " + recordMetadata.offset());
                        logger.info("data written at : " + recordMetadata.timestamp());
                    } else
                        logger.error("Error while producing records : " + e);

                }

            }).get();
            //block the data from .send to make it asynchronous do not so that in production (It is
            //just for learning)
            producer.flush();
//1. If you have only one partition then whether you define key or not data will obviously got to same one partition.
//2. If you keep the partition same like 3 or 4 mapping will be exactly same for the key.
//eg for the in all cases we will get mapping as above for five key in any system as same hashing algo is used
// Although if you change number of partition mapping would be different.
        }
    }
}
