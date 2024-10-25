package com.bgollako;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Main class for ZooKeeper operations.
 */
public class Main {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181"; // Change to your Zookeeper address
    private static final int SESSION_TIMEOUT = 3000;

    /**
     * Writes data to ZooKeeper nodes in parallel using multiple clients and returns the paths of the written nodes.
     * 
     * @param noOfClients The number of clients to use for writing to ZooKeeper nodes.
     * @param noOfZNodes The number of ZooKeeper nodes to write to.
     * @return A list of paths to the ZooKeeper nodes that were written.
     */
    public static List<String> writeZNodes(int noOfClients, int noOfZNodes) {
        Thread[] clients = new Thread[noOfClients];
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        List<String> znodePaths = new ArrayList<>();

        for (int i = 0; i < noOfZNodes; i++) { 
            znodePaths.add("/path/to/node/" + i);
        }

        int znodesPerClient = znodePaths.size() / noOfClients;
        int remainingZNodes = znodePaths.size() % noOfClients;

        for (int i = 0; i < noOfClients; i++) {
            final int clientIndex = i;
            clients[i] = new Thread(() -> {
                try {
                    ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, null);
                    int startIndex = clientIndex * znodesPerClient + Math.min(clientIndex, remainingZNodes);
                    int endIndex = startIndex + znodesPerClient + (clientIndex < remainingZNodes ? 1 : 0);

                    for (int j = startIndex; j < endIndex; j++) {
                        String znodePath = znodePaths.get(j);
                        long startTime = System.nanoTime();
                        zooKeeper.setData(znodePath, UUID.randomUUID().toString().getBytes(), -1);
                        long endTime = System.nanoTime();
                        long latency = endTime - startTime;
                        latencies.add(latency);
                        System.out.println("Write latency for " + znodePath + ": " + latency + " ns");
                    }
                    zooKeeper.close();
                } catch (IOException | KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
            clients[i].start();
        }

        for (Thread client : clients) {
            try {
                client.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        synchronized (latencies) {
            Collections.sort(latencies);
        }
        long totalLatency = latencies.stream().mapToLong(Long::longValue).sum();
        double avgLatency = totalLatency / (double) latencies.size();
        
        System.out.println("Total write latency: " + totalLatency + " ns");
        System.out.println("Average write latency: " + avgLatency + " ns");
        System.out.println("50th percentile write latency: " + getPercentile(latencies, 50) + " ns");
        System.out.println("90th percentile write latency: " + getPercentile(latencies, 90) + " ns");
        System.out.println("99.99th percentile write latency: " + getPercentile(latencies, 99.99) + " ns");

        return znodePaths; 
    }

    /**
     * Reads ZooKeeper nodes in parallel using multiple clients.
     * 
     * @param znodePaths The list of ZooKeeper node paths to read.
     * @param noOfClients The number of clients to use for reading ZooKeeper nodes.
     */
    public static void readZNodes(List<String> znodePaths, int noOfClients) {
        Thread[] clients = new Thread[noOfClients];
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        int znodesPerClient = znodePaths.size() / noOfClients;
        int remainingZNodes = znodePaths.size() % noOfClients;

        for (int i = 0; i < noOfClients; i++) {
            final int clientIndex = i;
            clients[i] = new Thread(() -> {
                try {
                    ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, null);
                    int startIndex = clientIndex * znodesPerClient + Math.min(clientIndex, remainingZNodes);
                    int endIndex = startIndex + znodesPerClient + (clientIndex < remainingZNodes ? 1 : 0);

                    for (int j = startIndex; j < endIndex; j++) {
                        String znodePath = znodePaths.get(j);
                        long startTime = System.nanoTime();
                        byte[] data = zooKeeper.getData(znodePath, null, null);
                        long endTime = System.nanoTime();
                        long latency = endTime - startTime;
                        latencies.add(latency);
                        System.out.println("Read latency for " + znodePath + ": " + latency + " ns");
                    }
                    zooKeeper.close();
                } catch (IOException | KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
            clients[i].start();
        }

        for (Thread client : clients) {
            try {
                client.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Collections.sort(latencies);
        long totalLatency = latencies.stream().mapToLong(Long::longValue).sum();
        double avgLatency = totalLatency / (double) latencies.size();
        
        System.out.println("Total read latency: " + totalLatency + " ns");
        System.out.println("Average read latency: " + avgLatency + " ns");
        System.out.println("50th percentile read latency: " + getPercentile(latencies, 50) + " ns");
        System.out.println("90th percentile read latency: " + getPercentile(latencies, 90) + " ns");
        System.out.println("99.99th percentile read latency: " + getPercentile(latencies, 99.99) + " ns");
    }

    /**
     * Calculates the latency at a given percentile from a list of latencies.
     * 
     * @param latencies The list of latencies.
     * @param percentile The percentile to calculate.
     * @return The latency at the specified percentile.
     */
    private static long getPercentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size()) - 1;
        return latencies.get(index);
    }

    /**
     * Main method to start the application.
     */
    public static void main(String[] args) {
        int numberOfClients = Integer.parseInt(args[0]); 
        int numberOfZNodes = Integer.parseInt(args[1]); 
        List<String> znodePaths = writeZNodes(numberOfClients, numberOfZNodes); 
        readZNodes(znodePaths, numberOfClients); 
    }
}
