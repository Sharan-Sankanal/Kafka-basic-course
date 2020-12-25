package com.github.simplekafka.practise;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger("ConsumerDemo");

        String bootserver = "127.0.0.1:9092";
        String topic = "FIRST_TOPIC";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootserver);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //Assign
        TopicPartition topicPartitionToRead = new TopicPartition(topic, 0);
        long offsetToRead = 2L;
        consumer.assign(Arrays.asList(topicPartitionToRead));

        consumer.seek(topicPartitionToRead, offsetToRead);

        int numberOfMsgToRead = 2;
        boolean keepReading = true;
        int numberofMsgReadSoFar = 0;
        //Poll for new data
        while(keepReading){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records){
                numberofMsgReadSoFar += 1;
                logger.info("Key: "+ record.key());
                logger.info("Value: "+ record.value());
                logger.info(("Partition: "+ record.partition()));
                logger.info("Offset: " + record.offset());
                if(numberofMsgReadSoFar >= numberOfMsgToRead){
                    keepReading = false;
                    break;
                }

            }
        }

        logger.info("Existing Application!");
    }
}
