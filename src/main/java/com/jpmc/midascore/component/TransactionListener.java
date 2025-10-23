package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import jakarta.transaction.Transactional;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class TransactionListener {

    RestTemplate restTemplate = new RestTemplate();
    String incentiveUrl = "http://localhost:8080/incentive";

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
        private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private BalanceCalculator balanceCalculator;

        private UserRepository userRepository;
        private TransactionRecordRepository transactionRecordRepository;

        // Will use this list to capture the first 4 amounts during debugging.
        private final List<Float> recordedAmounts = new ArrayList<>();

        public TransactionListener(UserRepository userRepository,
                               TransactionRecordRepository transactionRecordRepository) {
           this.userRepository = userRepository;
            this.transactionRecordRepository = transactionRecordRepository;
        }


        // The KafkaListener is configured to read from the topic defined by the
        // application property: general.kafka-topic.
        // Specify the Transaction class as the parameter type for automatic deserialization.
        @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
        @Transactional
        public void handleTransaction(Transaction transaction) {

            try {
                logger.info("Received Transaction: {}", transaction);
                recordedAmounts.add(transaction.getAmount());

                long senderId = transaction.getSenderId();
                long recipientId = transaction.getRecipientId();
                float amount = transaction.getAmount();

                boolean isValid = false;
                float incentiveAmount = 0f;

                UserRecord sender = userRepository.findById(senderId);
                UserRecord recipient = userRepository.findById(recipientId);

                if (sender == null || recipient == null) {
                    logger.warn("Validation failed: Sender ID {} or Recipient ID {} is invalid.", senderId, recipientId);
                } else if (sender.getBalance() < amount) {
                    logger.info("Validation failed: Insufficient funds for sender {} (Balance: {}) to send {}",
                            sender.getName(), sender.getBalance(), amount);
                } else {
                    ResponseEntity<Incentive> response = restTemplate.postForEntity(incentiveUrl, transaction, Incentive.class);
                    incentiveAmount = response.getBody().getAmount();

                    sender.setBalance(sender.getBalance() - amount);
                    recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);
                    userRepository.save(sender);
                    userRepository.save(recipient);
                    isValid = true;
                    logger.info("Transaction SUCCESS: {} -> {} for {}", sender.getName(), recipient.getName(), amount);
                }

                TransactionRecord record = new TransactionRecord(sender, recipient, amount, isValid, incentiveAmount);
                transactionRecordRepository.save(record);




            } catch (Exception e) {
                logger.error("Failed to process transaction: {}", transaction, e);
            }
        }

    public void logWaldorfBalance() {
        try {
            UserRecord waldorf = userRepository.findByName("waldorf");
            if (waldorf != null) {
                logger.info("Waldorf final balance: {}", (int) Math.floor(waldorf.getBalance()));
            } else {
                logger.warn("Waldorf user not found.");
            }
        } catch (Exception e) {
            logger.error("Error while fetching Waldorf balance", e);
        }
    }

    public void logAllFinalBalances() {
        try {
            Map<String, Integer> balances = balanceCalculator.calculateFinalBalances();
            logger.info("------ FINAL BALANCES ------");
            balances.forEach((name, balance) ->
                    logger.info("{}: {}", name, balance)
            );
        } catch (Exception e) {
            logger.error("Error while calculating final balances", e);
        }
    }
         /* A helper method to inspect the captured data, potentially useful in debugging.
         */
        public List<Float> getRecordedAmounts() {
            return recordedAmounts;
        }
}
