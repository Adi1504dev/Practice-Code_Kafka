import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Lesson_a0_KafkaElastic_setup {
    public static RestHighLevelClient createClient()
    {  final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        

    String host = "kafka-elastic-3185312576.eu-west-1.bonsaisearch.net";
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
    public static void main(String[] args) throws IOException {

        Logger logger= LoggerFactory.getLogger(Lesson_a0_KafkaElastic_setup.class.getName());
        RestHighLevelClient client1=createClient();
        String jsonData="{\"foo\":\"bar\"}";
        IndexRequest ir=new IndexRequest("twitter","Tweets").source(jsonData, XContentType.JSON);
        IndexResponse indexResponse=client1.index(ir, RequestOptions.DEFAULT);
        String id=indexResponse.getId();
        logger.info("Inserting into Elastic search doc");
        logger.info(id);
        logger.info("Closing client gracefully");
        client1.close();
        logger.info("Client Closed");

    }
}
