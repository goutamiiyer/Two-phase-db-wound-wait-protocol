package main.java;

import java.util.LinkedList;
import java.util.List;

public class Item {
    String itemName;
    String lockMode;
    List<Transaction> holdingTransactions;
    List<Transaction> waitingTransactions;
    public Item(String newItemName, String newLockType) {
        this.itemName = newItemName;
        this.lockMode = newLockType;
        holdingTransactions = new LinkedList<>();
        waitingTransactions = new LinkedList<>();
    }
}
