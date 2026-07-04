package com.jpmc.midascore.component;

import com.jpmc.midascore.IncentiveQuerier;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveQuerier incentiveQuerier;

    public KafkaTransactionListener(UserRepository userRepository,
                                    TransactionRepository transactionRepository,
                                    IncentiveQuerier incentiveQuerier) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveQuerier = incentiveQuerier;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(Transaction transaction) {

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        var incentive = incentiveQuerier.query(transaction);
        float incentiveAmount = incentive.getAmount();

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        sender.setBalance(sender.getBalance() - transaction.getAmount());

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentiveAmount
        );

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount(),
                        incentiveAmount
                );

        transactionRepository.save(record);
    }
}