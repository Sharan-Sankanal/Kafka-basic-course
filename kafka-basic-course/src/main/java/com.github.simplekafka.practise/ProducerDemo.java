package com.github.simplekafka.practise;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

       String bootstrapServer = "127.0.0.1:9092";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //Create a producer read
        ProducerRecord<String,String> record = new ProducerRecord<>("FIRST_TOPIC", "Hello world!!!");
        //Send Data
        producer.send(record);
        producer.flush();
        producer.close();

    }
}
