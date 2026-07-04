package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
    private final String topic;
    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public KafkaProducer(@Value("${general.kafka-topic}") String topic, KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
//        System.out.println("RAW: [" + transactionLine + "]");

        String[] transactionData = transactionLine.split(",");

//        System.out.println("Length = " + transactionData.length);
//        for (int i = 0; i < transactionData.length; i++) {
//            System.out.println(i + " = [" + transactionData[i] + "]");
//        }

        kafkaTemplate.send(
                topic,
                new Transaction(
                        Long.parseLong(transactionData[0].trim()),
                        Long.parseLong(transactionData[1].trim()),
                        Float.parseFloat(transactionData[2].trim())
                )
        );
    }
}