package main.java;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class Transaction {
    String id;
    String operation;
    String state;
    LocalDateTime localDateTime;
    List<Item> itemsLocked;
    public Transaction(String transactionId, String transactionState){
        id = transactionId;
        state = transactionState;
        localDateTime = LocalDateTime.now();
        itemsLocked = new LinkedList<>();
    }
}

