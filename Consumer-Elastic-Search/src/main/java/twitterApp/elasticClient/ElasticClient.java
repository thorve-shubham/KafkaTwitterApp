package twitterApp.elasticClient;

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
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitterApp.consumer.TwitterConsumer;


import java.io.IOException;

public class ElasticClient {

    public static final int port = 443;
    public static final String conn = "https";
    public static final String indexName = "twitter";

    static {

    }



    public static RestHighLevelClient getElasticClient(){

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(TwitterConsumer.appProperties.getProperty("elastic.username"),TwitterConsumer.appProperties.getProperty("elastic.password")));

        RestClientBuilder builder = RestClient.builder(new HttpHost(TwitterConsumer.appProperties.getProperty("elastic.host"),port,conn))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return  client;

    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticClient.class.getName());
        logger.info("creating Elastic rest client");
        RestHighLevelClient client = ElasticClient.getElasticClient();

        String JsonData = "{\n" +
                "  \"name\" : \"Shubam Thorve\",\n" +
                "  \"Profession\" : \"Software Developer\"\n" +
                "}";

        logger.info("Creating indexRequest....");
        IndexRequest request = new IndexRequest(indexName).source(JsonData, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        String id = response.getId();
        String index = response.getIndex();

        logger.info("\n#id : "+id+"\nIndex : "+index);
         client.close();
    }
}
