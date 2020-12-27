package com.github.simplekafka.practise;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProduceDemoWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProduceDemoWithCallback.class.getName());
        String bootstrapServer = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<5; i++){
            //Create produer record
            ProducerRecord<String, String> record = new ProducerRecord<>("FIRST_TOPIC", "Hello World " + i);

            producer.send(record, (recordMetadata, e) -> {
                if(e == null){
                    logger.info("Kafka producer details: \n" +
                            "Topic: "+ recordMetadata.topic() + "\n"+
                            "Partition: "+ recordMetadata.partition() + "\n" +
                            "Offset: "+ recordMetadata.offset() + "\n"+
                            "Timestamp: " + recordMetadata.timestamp());
                }else{
                    logger.error("Error while producing : "+ e);
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
