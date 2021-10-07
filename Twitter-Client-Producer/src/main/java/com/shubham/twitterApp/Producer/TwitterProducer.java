package com.shubham.twitterApp.Producer;

import com.shubham.twitterApp.twitterClient.TwitterClient;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    public static Properties appProperties =new Properties();

    static {
        try {
            FileReader reader=new FileReader("src/main/resources/application.properties");
            appProperties.load(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static KafkaProducer<String,String> getKafkaProducer(){
        //Setting Up Producer Properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,appProperties.getProperty("kafka.server.url"));
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //creating a safe producer
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        //high throughput producer config with compression and batches
        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); //32 KB

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(producerProperties);
        return producer;
    }

    public static void main(String[] args){
        Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

        logger.info("Creating Twitter Client....");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client myClient = TwitterClient.getTwitterClient(msgQueue);

        logger.info("Connecting to twitter Stream");
        myClient.connect();

        //Create kafka Producer
        KafkaProducer<String,String> producer = getKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping Application...");
            logger.info("Stopping Twitter Client...");
            myClient.stop();
            logger.info("Stopping Producer...");
            producer.close();
        }));

        logger.info("Reading through Kafka Streams...");
        while(!myClient.isDone()){
            String tweet = null;
            try {
                tweet = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error("Exception Occurred Closing Twitter Client");
                myClient.stop();
                producer.close();
            }
            logger.info("\n#TWEET : \n"+tweet);
            producer.send(new ProducerRecord<String,String>("twitterTopic",tweet));
        }
    }
}
