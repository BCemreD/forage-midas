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

    public void send(String transactionLine){
        String[] transactionData = transactionLine.trim().split(",\\s*");
        if (transactionData.length != 3) {
            throw new IllegalArgumentException("Invalid transaction format: " + transactionLine);
        }
        try {
            long senderId = Long.parseLong(transactionData[0]);
            long recipientId = Long.parseLong(transactionData[1]);
            float amount = Float.parseFloat(transactionData[2].replaceAll("[^0-9.]", ""));
            kafkaTemplate.send(topic, new Transaction(senderId, recipientId, amount));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse transaction line: " + transactionLine, e);
        }
    }

}