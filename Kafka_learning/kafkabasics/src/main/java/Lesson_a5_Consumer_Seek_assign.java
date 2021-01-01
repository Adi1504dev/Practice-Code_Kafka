import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Lesson_a5_Consumer_Seek_assign {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Lesson_a5_Consumer_Seek_assign.class);
        //Latch for Dealing with multiple threads
        //We decrease the latch count to zero once we close the application by shutdown function.
        //Current function will be in wait state until latch count is zero.
        CountDownLatch latch = new CountDownLatch(1);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //no group id is used here
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
       //assign
        TopicPartition topictoreadfrom=new TopicPartition("first_topic",1);
        consumer.assign(Arrays.asList(topictoreadfrom));//we can pass collection of topic partition

        //seek -- sets offset value from where data is to be read.
        Long offsettoreadfrom=15l;
        consumer.seek(topictoreadfrom,offsettoreadfrom);

        Runnable consumerRunnable1=new ConsumerRunnable1(latch,consumer);
        Thread t = new Thread(consumerRunnable1);
        t.start();

        //Add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            //This is way to do a task once the application is closed forcefully
            logger.info("Caught in ShutDown hook");
            ((ConsumerRunnable1)consumerRunnable1).shutdown();
        }));

        try {
            //Function wait until latch is zero
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application interrupted ", e);
        } finally {
            logger.info("Closing the application");
        }
    }

}

class ConsumerRunnable1 implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable1.class);
    private final KafkaConsumer<String, String> consumer;
    private Properties properties;
    private final CountDownLatch latch;

    public ConsumerRunnable1(CountDownLatch latch, KafkaConsumer<String, String> consumer) {
        this.latch = latch;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        //Say we only want to read 5 messages

        int nosOfmsgtoread=5;
        boolean readflag=true;
        int numberofmessageread=0;
       try {
           while (readflag) {
               ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
               for (ConsumerRecord record : records) {
                   numberofmessageread+=1;
                   logger.info("Number of Messages Read so far : "+numberofmessageread);
                   logger.info("Key: " + record.key() + " Partition: " + record.partition() + " Topic: " + record.topic());
                   logger.info("offset Value: " + record.offset() + " Value: " + record.value());
                   if(nosOfmsgtoread==numberofmessageread)
                   {
                       readflag=false;
                       break;
                   }
               }

           }
       } catch (WakeupException e) {
           logger.info("Closing Signal Received");
       }
       finally {
           consumer.close();
           latch.countDown();
       }

    }

    public void shutdown() {
    consumer.wakeup();
    }
}