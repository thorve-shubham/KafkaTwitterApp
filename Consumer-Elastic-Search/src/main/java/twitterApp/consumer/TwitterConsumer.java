package twitterApp.consumer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitterApp.elasticClient.ElasticClient;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumer {

    public static Properties appProperties =new Properties();

    static {
        try {
            FileReader reader=new FileReader("src/main/resources/application.properties");
            appProperties.load(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static KafkaConsumer<String,String> getConsumer(String topic){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,appProperties.getProperty("kafka.server.url"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"kafka-twitter-elastic");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");  //disabling auto commit
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"5"); //max 5 records on each batch

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static String getTweetId(String jsonTweet) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject tweet = (JSONObject) parser.parse(jsonTweet);
        return tweet.get("id_str").toString();
    }

    public static void main(String[] args) throws IOException, InterruptedException, ParseException {
        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);

        logger.info("Creating Kafka Consumer.....");
        KafkaConsumer<String,String> consumer = getConsumer("twitterTopic");
        logger.info("Creating Elastic Search Client");
        RestHighLevelClient client = ElasticClient.getElasticClient();
        String indexName = "twitter";
        int i=0;
        while(i<10){
            ConsumerRecords<String,String> records = consumer.poll(100);
            logger.info("RETRIEVED BATCH : Records : "+records.count());
            //improving performance by passing all records at once to elastic search
            BulkRequest bulkRequest = new BulkRequest();
            for(ConsumerRecord<String,String> record : records){
                i++;
                if(i>=10)
                    break;
                //Adding an Id for making o=consumer idempotence so no duplicate records gets added in elastic search
                //Method#1
                //User String as = record.topic()+"_"+record.partition()+"_"+record.offset();
                //Method#2
                //Use Tweet Id to make sure no duplicates will get processed by elastic search
                try{
                    IndexRequest request = new IndexRequest(indexName).source(record.value(), XContentType.JSON);
                    request.id(getTweetId(record.value()));
                    bulkRequest.add(request);
                    Thread.sleep(10);
                }catch(NullPointerException e){
                    logger.warn("Skippin invalid data....");
                }

            }
            if(records.count()>0){
                BulkResponse response = client.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("Processed  Batch and now committing offsets..");
                //commiting offsets manually for each poll
                consumer.commitSync();
                Thread.sleep(1000);
            }

        }



        logger.info("Closing  Elastic Client");
        client.close();

        logger.info("Closing  Kafka Consumer");
        consumer.close();
    }
}
