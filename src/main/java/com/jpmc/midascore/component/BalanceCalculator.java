package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Component
public class BalanceCalculator {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    public Map<String, Integer> calculateFinalBalances() {
        Map<String, Float> balanceMap = new HashMap<>();

        // Initial balances
        for (UserRecord user : userRepository.findAll()) {
            balanceMap.put(user.getName(), user.getBalance());
        }


        for (TransactionRecord tx : transactionRecordRepository.findAllWithUsers()) {
            if (!tx.isValid()) continue;

            String sender = tx.getSender().getName();
            String recipient = tx.getRecipient().getName();
            float amount = tx.getAmount();
            float incentive = tx.getIncentive();

            balanceMap.put(sender, balanceMap.get(sender) - amount);
            balanceMap.put(recipient, balanceMap.get(recipient) + amount + incentive);
        }

        // Rounded down to the nearest integer.
        return balanceMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> (int) Math.floor(e.getValue()),
                        (a, b) -> b,
                        TreeMap::new
                ));
    }
}