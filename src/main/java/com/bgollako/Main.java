package com.bgollako;

import com.bgollako.latency.Latency;
import java.util.List;
import com.bgollako.contest.Contestants;

/**
 * Main class for ZooKeeper operations.
 * This class provides a starting point for the application, allowing for the execution of ZooKeeper operations.
 */
public class Main {
    /**
     * The address of the ZooKeeper server to connect to.
     * This should be replaced with the actual ZooKeeper server address in your environment.
     */
    public static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    /**
     * The session timeout in milliseconds for ZooKeeper connections.
     */
    public static final int SESSION_TIMEOUT = 3000;

    /**
     * Main method to start the application.
     * This method initiates the ZooKeeper operations by calling the testContest method.
     * 
     * @param args Command line arguments (not used in this application)
     */
    public static void main(String[] args) {
        testContest();
    }

    /**
     * Method to test the contest functionality.
     * This method creates a Contestants instance and starts it in a new thread.
     * It then waits for the thread to finish.
     */
    static void testContest() {
        Contestants contestants = new Contestants(
            ZOOKEEPER_ADDRESS, 
            "/contest", 
            3
        );
        Thread thread = new Thread(() -> {
            try {
                contestants.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();

    }

    /**
     * Method to test the latency of ZooKeeper operations.
     * This method simulates the creation, reading, and deletion of ZooKeeper nodes to measure latency.
     */
    static void testLatency() {
        int numberOfClients = 1; // Number of clients to simulate
        int numberOfZNodes = 10000; // Number of ZooKeeper nodes to create
        List<String> znodePaths = Latency.writeZNodes(numberOfClients, numberOfZNodes); // Create ZooKeeper nodes
        Latency.readZNodes(znodePaths, numberOfClients); // Read the created ZooKeeper nodes
        Latency.deleteZNodes(znodePaths, numberOfClients); // Delete the created ZooKeeper nodes
    }
}
