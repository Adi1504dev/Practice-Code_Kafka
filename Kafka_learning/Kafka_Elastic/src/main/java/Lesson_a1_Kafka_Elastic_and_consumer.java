import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Lesson_a1_Kafka_Elastic_and_consumer {
    public static RestHighLevelClient createClient()
    {  final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();


        String host = "kafka-elastic-3185312576.eu-west-1.bonsaisearch.net";//remove the port from the end
        String User = "43loyvs25d";
        String Password = "zvqrylkh00";
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(User, Password));
        RestClientBuilder builder= RestClient.builder(new HttpHost(host, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        RestHighLevelClient client=new RestHighLevelClient(builder);
        return client;
    }
    public static KafkaConsumer<String,String> createConsumer(String topic)
    {
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Kafka_Consumner_ElasticSearch");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //earliest --> Read from Beginning
        //latest -->Read the latest produced after consumer starts running
        //none--> will throw an error if there no offsets being saved(not used often)

        //Create a consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger= LoggerFactory.getLogger(Lesson_a1_Kafka_Elastic_and_consumer.class.getName());
        RestHighLevelClient client1=createClient();



        KafkaConsumer<String,String> consumer=createConsumer("twitter_tweets");
        while(true)
        {

            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : records)   //Record not Records
            {
                String jsonData= record.value();
                IndexRequest ir=new IndexRequest("twitter","Tweets").source(jsonData, XContentType.JSON);
                IndexResponse indexResponse=client1.index(ir, RequestOptions.DEFAULT);
                String id=indexResponse.getId();
                logger.info("Inserting into Elastic search doc");
                logger.info(id);
                Thread.sleep(1000);
            }
        }
    }
}

