import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Lesson_a4_Consumer_With_Thread_Adv {
    public static void main(String[] args) {
        new Lesson_a4_Consumer_With_Thread_Adv().runner();
    }
    public void runner() {
        Logger logger = LoggerFactory.getLogger(Lesson_a4_Consumer_With_Thread_Adv.class);
        //Latch for Dealing with multiple threads
        //We decrease the latch count to zero once we close the application by shutdown function.
        //Current function will be in wait state until latch count is zero.
        CountDownLatch latch = new CountDownLatch(1);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "My_Consumer_Application_Thread_adv");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribing the topic
        consumer.subscribe(Arrays.asList("first_topic"));

        //Making consumer runnable
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(consumer, latch);
        Thread t = new Thread(myConsumerRunnable);
        t.start();

        //Add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            //This is way to do a task once the application is closed
            logger.info("Caught in ShutDown hook");
            myConsumerRunnable.shutdown();
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

    class ConsumerRunnable implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private final KafkaConsumer<String, String> consumer;
        private Properties properties;
        private final CountDownLatch latch;

        public ConsumerRunnable(KafkaConsumer<String, String> consumer, CountDownLatch latch) {
            this.consumer = consumer;
            this.latch = latch;
        }

        @Override
        public void run() {
            //Poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records)   //Record not Records
                    {
                        logger.info("Key: " + record.key() + " Partition: " + record.partition() + " Topic: " + record.topic());
                        logger.info("offset Value: " + record.offset() + " Value: " + record.value());
                    }

                }
            } catch (WakeupException e) {
                logger.info("Received the closing singnal");
            } finally {
                consumer.close();
                //making main method know that we have closed the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            //Raise Exception wakeup --> A way to stop consumer
            consumer.wakeup();
        }
    }
}

