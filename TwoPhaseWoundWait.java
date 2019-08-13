package main.java;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TwoPhaseWoundWait {
    public static void main(String[] args) {
        List<String> contents = fileReader();
        System.out.println(contents);
        parseContents(contents);
    }

    /**
     * Reads the contents of the input schedule file
     * @return
     */
    public static List<String> fileReader()
    {
        BufferedReader reader;
        List<String> inputList = new LinkedList<>();
        try {
            reader = new BufferedReader(new FileReader("C:\\Users\\deepa\\Desktop\\DB2\\input2.txt"));
            String line = reader.readLine();
            while (line != null) {
                inputList.add(line);
                line = reader.readLine();
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return inputList;
    }

    /**
     * Parses the contents present in schedule file and differentiates the type of operations such as begin, read, write, end
     * @param inputList
     */
    private static void parseContents(List<String> inputList) {
        String transactionid;
        String itemLocked;
        Transaction transaction;
        Item item;
        Map<String, Transaction> transactionMap = new HashMap<>();
        for (int i = 0; i < inputList.size(); i++){
            String inputItem = inputList.get(i);
            char inputOperation = inputItem.charAt(0);
            transactionid = String.valueOf(inputItem.charAt(1));    // Integer.parseInt(String.valueOf(inputItem.charAt(1)));  //transaction id at position 1
            switch (inputOperation) {
                case 'b':
                    transaction = new Transaction(transactionid, "ACTIVE");
                    transaction.operation = "STARTED";
                    transactionMap.put(transactionid, transaction);
                    System.out.println("Starting transaction " + transaction.id);
                    System.out.println("Transaction state of " + transaction.id+ " is " + transaction.state);
                    System.out.println("Transaction " + transaction.id + " was created at " + transaction.localDateTime);
                    break;
                case 'r':
                    transaction = transactionMap.get(transactionid);
                    item = new Item(String.valueOf(inputItem.charAt(3)), "READ_LOCK");
                    readOperation(item, transaction);
                    break;
                case 'w':
                    transaction = transactionMap.get(transactionid);
                    item = new Item(String.valueOf(inputItem.charAt(3)), "WRITE_LOCK");
                    writeOperation(item, transaction);
                    break;
                case 'e':
                    transaction = transactionMap.get(transactionid);
                    if(transaction.state == "ABORTED") {
                        System.out.println("Transaction " + transactionid + " has already been aborted");
                        break;
                    }
                    transaction.state = "COMMIT";
                    transaction.operation = "ENDED";
                    for(int itemNum = 0; itemNum < transaction.itemsLocked.size(); itemNum++) {
                        item = transaction.itemsLocked.get(itemNum);
                        item.lockMode = "UNLOCKED"; // Unlocks the current item
                        item.holdingTransactions.remove(transaction);

                        if(!item.waitingTransactions.isEmpty()) {
                            transaction = item.waitingTransactions.get(0);
                            if(transaction.operation == "READ" && transaction.state == "BLOCKED") {
                                readOperation(item, transaction);
                            } else if(transaction.operation == "WRITE" && transaction.state == "BLOCKED") {
                                writeOperation(item, transaction); //write operation;
                            }
                        }
                    }
                    transactionMap.remove(transactionid);
                    System.out.println("Transaction " + transaction.id + " ended ");
                    System.out.println("Transaction state of " + transaction.id + " is " + transaction.state);
                    break;
                default:
                    System.out.println("Operation is not valid");
                    return;
            }
        }
    }

    /**
     * If the requesting operation on an item is read then READ LOCK is established on the item and transaction operation is set to READ.
     * It also follows the wound wait protocol based on schedule
     * @param item
     * @param transaction
     */
    private static void readOperation(Item item, Transaction transaction) {
        if(transaction.state == "ABORTED") {
            System.out.println("Transaction " + transaction.id + " has already been aborted");
            return;
        }
        if(item.holdingTransactions.isEmpty()) {
            System.out.println("No holding transactions on " + item.itemName);
            transaction.itemsLocked.add(item);
            transaction.operation = "READ";
            item.holdingTransactions.add(transaction);
            item.lockMode = "READ_LOCK";
            System.out.println("Transaction " + transaction.id + " acquires READ LOCK on " + item.itemName);
        } else {
            int trans;
            for(trans = 0; trans < item.holdingTransactions.size(); trans++) {
                if(item.holdingTransactions.get(trans).operation == "WRITE") {
                    if(item.holdingTransactions.get(trans).id == transaction.id) {
                        //downgrade the transaction
                        transaction.operation = "READ";
                        item.lockMode = "READ_LOCK";
                        System.out.println("Transaction " + transaction.id + "downgrades WRITE LOCK to READ LOCK on " + item.itemName);
                    }
                    else {
                        woundWait(item.holdingTransactions.get(trans), transaction, "READ", "READ_LOCK");
                        break;
                    }
                } else {
                        transaction.itemsLocked.add(item);
                        transaction.operation = "READ";
                        item.holdingTransactions.add(transaction);
                        item.lockMode = "READ_LOCK";
                }
            }
//            if(trans == item.holdingTransactions.size()) {
//                transaction.itemsLocked.add(item);
//                transaction.operation = "READ";
//                item.holdingTransactions.add(transaction);
//                item.lockMode = "READ_LOCK";
//            }
        }
    }

    /**
     * If the requesting operation on an item is write then WRITE LOCK is established on the item and transaction operation is set to WRITE
     * It also follows the wound wait protocol based on schedule
     * @param item
     * @param transaction
     */
    private static void writeOperation(Item item, Transaction transaction) {
        if(transaction.state == "ABORTED") {
            System.out.println("Transaction " + transaction.id + " has already been aborted");
            return;
        }
        if (item.holdingTransactions.isEmpty()) {
            transaction.itemsLocked.add(item);
            transaction.operation = "WRITE";
            item.holdingTransactions.add(transaction);
            item.lockMode = "WRITE_LOCK";
            System.out.println("Transaction " + transaction.id + " upgrades READ LOCK to WRITE LOCK on " + item.itemName);
        } else {
            int trans;
            for (trans = 0; trans < item.holdingTransactions.size(); trans++) {
                if (item.holdingTransactions.get(trans).operation == "READ") {
                    if(item.holdingTransactions.get(trans).id == transaction.id) {
                        // upgrade the transaction
                        transaction.operation = "WRITE";
                        item.lockMode = "WRITE_LOCK";
                        System.out.println("Transaction " + transaction.id + "upgrades READ LOCK to WRITE LOCK on " + item.itemName);

                    } else {
                        woundWait(item.holdingTransactions.get(trans), transaction, "WRITE", "WRITE_LOCK");
                        break;
                    }
                }
            }
        }
    }

    /**
     * Timestamp of the requesting and holding transactions are compared and one of the transactions waits, aborts or acquires read/write lock based on wound wait algorithm
     * @param holdingTransaction
     * @param requestingTransaction
     * @param operation
     * @param lockMode
     */
    private static void woundWait(Transaction holdingTransaction, Transaction requestingTransaction, String operation, String lockMode) {
        if (holdingTransaction.localDateTime.compareTo(requestingTransaction.localDateTime) < 0) {
            // requestingTransaction waits
            requestingTransaction.state = "BLOCKED";
            System.out.println("By following wound-wait protocol, transaction " + requestingTransaction.id + " is blocked");
        } else {
            // holdingTransaction aborts
            holdingTransaction.state = "ABORTED";
            // requestingTransaction.state = "ACTIVE";
            requestingTransaction.itemsLocked.add(holdingTransaction.itemsLocked.get(0)); // Item added to requesting transaction from holding transaction
            System.out.println("Item" + holdingTransaction.itemsLocked.get(0) + " will be unlocked by transaction" + holdingTransaction.id + " after abort");
            holdingTransaction.itemsLocked.remove(0); // Item removed from holding transaction.
            System.out.println("By following wound-wait protocol, transaction " + holdingTransaction.id + " is aborted");
        }
    }
}
