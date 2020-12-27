package com.github.simplekafka.practise;

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

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-frist-application";
        String topic = "FIRST_TOPIC";

        //Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //Create the consumer runnable
        logger.info("Creating the consumer thread!!! ");
         Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServer,
                groupId,
                topic,
                latch);

         //Start the thread
         Thread myThread = new Thread(myConsumerRunnable);
         myThread.start();

         // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread ( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Application is closing");
        }

    }

}

class ConsumerRunnable implements Runnable{

    private  CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.latch= latch;
        consumer = new KafkaConsumer<>(properties);
        //Subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        }

    @Override
    public void run() {
        try{
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record: records) {
                    logger.info("Key: "+ record.key());
                    logger.info("Value: "+ record.value());
                    logger.info("Partition: "+ record.partition());
                    logger.info("Offsert: "+ record.offset());
                }
            }
        }catch (WakeupException e){
            logger.error("Shutdown action received!!");
        }finally {
            logger.info("Calling shutdown!!!!");
            consumer.close();
            //Inform main method we are done with consumer
            latch.countDown();
        }
    }
    public void shutdown(){
        //the wakeup() method is a special method to interrupt consumer.poll()
        //it will throw the WakeupException
        consumer.wakeup();
    }
}