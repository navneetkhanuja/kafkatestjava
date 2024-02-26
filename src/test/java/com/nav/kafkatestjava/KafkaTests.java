package com.nav.kafkatestjava;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(
        brokerProperties = "localhost:9092",
        topics = { "TestTopic"},
        partitions = 1)
public class KafkaTests {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    DefaultKafkaConsumerFactory<String, String> defaultKafkaConsumerFactory;

    @Test
    void testKafkaMsgFlow () {
        System.out.println("Sending message to kafka topic");

        kafkaTemplate.send("TestTopic","Testing kafka msg flow");

        kafkaTemplate.setConsumerFactory(defaultKafkaConsumerFactory);

        ConsumerRecord<String, String> record = kafkaTemplate.receive("TestTopic", 0,0);

        System.out.println("Message Received: " + record.value());
    }

}
