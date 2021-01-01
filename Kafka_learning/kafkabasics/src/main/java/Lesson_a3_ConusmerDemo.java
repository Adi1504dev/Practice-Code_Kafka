
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Lesson_a3_ConusmerDemo {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(Lesson_a3_ConusmerDemo.class);
       //Set consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"My_Consumer_Application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
                //earliest --> Read from Beginning
                //latest -->Read the latest produced after consumer starts running
                //none--> will throw an error if there no offsets being saved(not used often)

        //Create a consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(properties);

        //Subuscribe consumer to our topic
        consumer.subscribe(Arrays.asList("first_topic"));
        //Can add multiple topics to list


        //Poll for new data
        /*Consumer do not get data until it asks for data*/
        while(true) /*bad practice in programming should not be used often*/
        {
            //Language has to be set to lambda 8 by alt+enter can be checked in XML file
           ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
            /*
            * The value Duration define the how quickly you want your poll method to return with or without data
            * poll handle all co-ordinations, partition re-balances and heart beat for group coordinator
            * when we call poll for first time
            *   1. Connect to group coordinator
            *   2. joins the group
            *   3. Receives partitions assignment
            *   4. sends heartbeats
            *   5 Fetches your messages and many more things  */


           for(ConsumerRecord<String,String> record : records)   //Record not Records
           {
                  logger.info("Key: "+ record.key()+" Partition: "+record.partition()+" Topic: " +record.topic());
                  logger.info("offset Value: "+record.offset()+" Value: " +record.value());
           }
        }

    }

    /*
    * Consumer re-balancing
    * By running the multiple instances of consumer class having same consumer group we create multiple consumer of a group
    * Every time we run new consumer class new consumer is added to group and partition re-balance occurs
    * Say we have 2 consumers
    * consumer 1- partition1 and consumer 2 - partition2 and partition0
    * now we run another instance of consumer class and re-balance will occur
    * consumer 1- partition0 and consumer 2 - partition0 and consumer3-partition0
    * in-case some consumer dies the (we close one of the program) another consumer recognize it by poll
    * and again re-balancing occurs
    *  */

}
