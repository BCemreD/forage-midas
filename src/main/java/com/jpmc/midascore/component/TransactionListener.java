package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


    @Component
    public class TransactionListener {

        private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
        private final ObjectMapper objectMapper = new ObjectMapper();

        // Will use this list to capture the first 4 amounts during debugging.
        private final java.util.List<Float> recordedAmounts = new java.util.ArrayList<>();

        // The KafkaListener is configured to read from the topic defined by the
        // application property: general.kafka-topic.
        // Specify the Transaction class as the parameter type for automatic deserialization.
        @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
        public void handleTransaction(String message) {
            try {
                logger.info("Raw Kafka message: '{}'", message);//raw message

                // End line
                String cleanMessage = message.trim().replace("\n", "").replace("\r", "");
                logger.info("Cleaned Kafka message: '{}'", cleanMessage);

                // This is the line where you will set your BREAKPOINT in the debugger.
                // The first four times the debugger stops here, record the amount.
                Transaction transaction = objectMapper.readValue(cleanMessage, Transaction.class);
                recordedAmounts.add(transaction.getAmount());


                logger.info("Received Transaction: {}", transaction);

            } catch (Exception e) {
                logger.error("Failed to parse transaction message: {}", message, e);
            }
        }
        /**
         * A helper method to inspect the captured data, potentially useful in debugging.
         */
        public java.util.List<Float> getRecordedAmounts() {
            return recordedAmounts;
        }
}
