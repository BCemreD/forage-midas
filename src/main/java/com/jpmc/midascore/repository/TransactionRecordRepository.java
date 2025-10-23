package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.TransactionRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface TransactionRecordRepository extends JpaRepository<TransactionRecord,Long> {

    @Query("SELECT t FROM TransactionRecord t JOIN FETCH t.sender JOIN FETCH t.recipient")
    List<TransactionRecord> findAllWithUsers();
}


