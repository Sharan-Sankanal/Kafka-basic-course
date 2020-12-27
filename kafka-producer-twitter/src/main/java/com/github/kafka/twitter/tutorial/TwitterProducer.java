package com.github.kafka.twitter.tutorial;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    List<String> terms = Lists.newArrayList("Kafka");

    public TwitterProducer() {}
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){

        //Create a Twitter Client
        logger.info("Twitter Kakfa Setup");
        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();


        //Create a Kafka producer
        KafkaProducer<String, String>  producer = createKafkaProducer();

        //add shutdown hoot
        Runtime.getRuntime().addShutdownHook(new Thread( () ->{
            logger.info("Stopping application...");
            client.stop();
            logger.info("Stoppping Kafka producer...");
            producer.close();
            logger.info("Application stopped!");
        }));

        //Loop through the message and send tweets to kafka
        String msg=null;
        while (!client.isDone()) {
            try {
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Eror!! Stopping Twitter Client {0}", e);
                client.stop();
            }
            if(msg !=null){
                logger.info("Received the Tweet: {}", msg);
                producer.send(new ProducerRecord<>("TWITTER_TWEET", null, msg), (recordMetadata, e) -> {
                    if(e != null){
                        logger.info("Topic: "+ recordMetadata.topic());
                        logger.info("Offset: "+ recordMetadata.offset());
                        logger.info("Partition: "+ recordMetadata.partition());
                        logger.error("Error: "+ e);
                    }
                });
            }
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServer = "127.0.0.1:9092";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create a Safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5)); // Kafaka version >= 1.0, then we can use 5 if not, use only 1.
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString((Integer.MAX_VALUE)));

        //High throughput producer(at the expense of bit latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        //Create the producer
        return new KafkaProducer<>(properties);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        //Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        String CONSUMER_KEY = "JZ1Qjdfq7lNjBEPnTebOooHqO";
        String TOKEN = "1342922058074890240-exCTq5JeK05m0qGiRUPmYrwy6IUUCL";
        String CONSUMER_SECRET = "LufZnV8CwuXDwGpOMTlrONA9qulL5d9t0kOmi8iPaBtfps31Uq";
        String SECRET = "aatdhBBkhmzfiaA05oeYL62RYI0t1UIHD9xHwo65pE5rM";
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);


        ClientBuilder builder = new ClientBuilder()
                .name("Kafka-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }
}

