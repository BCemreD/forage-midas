package com.jpmc.midascore.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Many transactions to one sender
    // We assume the UserRecord class exists and is the target.
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sender_id") // ID will be null if user lookup failed
    private UserRecord sender;

    // Many transactions to one recipient
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "recipient_id") // ID will be null if user lookup failed
    private UserRecord recipient;
    private float amount;
    private boolean isValid;
    private LocalDateTime transactionTimestamp = LocalDateTime.now();

    // Constructors
    public TransactionRecord() {}

    public TransactionRecord(UserRecord sender, UserRecord recipient, float amount, boolean isValid) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
        this.isValid = isValid;
    }

}