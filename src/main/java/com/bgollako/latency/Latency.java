package com.bgollako.latency;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.bgollako.Main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;

public class Latency {
    private static final Logger logger = Logger.getLogger(Latency.class);

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
            znodePaths.add("/" + i);
        }

        int znodesPerClient = znodePaths.size() / noOfClients;
        int remainingZNodes = znodePaths.size() % noOfClients;

        for (int i = 0; i < noOfClients; i++) {
            final int clientIndex = i;
            clients[i] = new Thread(() -> {
                try {
                    ZooKeeper zooKeeper = new ZooKeeper(Main.ZOOKEEPER_ADDRESS, Main.SESSION_TIMEOUT, null);
                    int startIndex = clientIndex * znodesPerClient + Math.min(clientIndex, remainingZNodes);
                    int endIndex = startIndex + znodesPerClient + (clientIndex < remainingZNodes ? 1 : 0);

                    for (int j = startIndex; j < endIndex; j++) {
                        String znodePath = znodePaths.get(j);
                        long startTime = System.nanoTime();
                        zooKeeper.create(znodePath, UUID.randomUUID().toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        long endTime = System.nanoTime();
                        long latency = endTime - startTime;
                        latencies.add(latency);
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
        
        logger.info("Total write latency: " + totalLatency + " ns");
        logger.info("Average write latency: " + avgLatency + " ns");
        logger.info("50th percentile write latency: " + getPercentile(latencies, 50) + " ns");
        logger.info("90th percentile write latency: " + getPercentile(latencies, 90) + " ns");
        logger.info("99.99th percentile write latency: " + getPercentile(latencies, 99.99) + " ns");

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
                    ZooKeeper zooKeeper = new ZooKeeper(Main.ZOOKEEPER_ADDRESS, Main.SESSION_TIMEOUT, null);
                    int startIndex = clientIndex * znodesPerClient + Math.min(clientIndex, remainingZNodes);
                    int endIndex = startIndex + znodesPerClient + (clientIndex < remainingZNodes ? 1 : 0);

                    for (int j = startIndex; j < endIndex; j++) {
                        String znodePath = znodePaths.get(j);
                        long startTime = System.nanoTime();
                        byte[] data = zooKeeper.getData(znodePath, null, null);
                        long endTime = System.nanoTime();
                        long latency = endTime - startTime;
                        latencies.add(latency);
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
        
        logger.info("Total read latency: " + totalLatency + " ns");
        logger.info("Average read latency: " + avgLatency + " ns");
        logger.info("50th percentile read latency: " + getPercentile(latencies, 50) + " ns");
        logger.info("90th percentile read latency: " + getPercentile(latencies, 90) + " ns");
        logger.info("99.99th percentile read latency: " + getPercentile(latencies, 99.99) + " ns");
    }

    /**
     * Deletes ZooKeeper nodes in parallel using multiple clients.
     * 
     * @param znodePaths The list of ZooKeeper node paths to delete.
     * @param noOfClients The number of clients to use for deleting ZooKeeper nodes.
     */
    public static void deleteZNodes(List<String> znodePaths, int noOfClients) {
        Thread[] clients = new Thread[noOfClients];
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        int znodesPerClient = znodePaths.size() / noOfClients;
        int remainingZNodes = znodePaths.size() % noOfClients;

        for (int i = 0; i < noOfClients; i++) {
            final int clientIndex = i;
            clients[i] = new Thread(() -> {
                try {
                    ZooKeeper zooKeeper = new ZooKeeper(Main.ZOOKEEPER_ADDRESS, Main.SESSION_TIMEOUT, null);
                    int startIndex = clientIndex * znodesPerClient + Math.min(clientIndex, remainingZNodes);
                    int endIndex = startIndex + znodesPerClient + (clientIndex < remainingZNodes ? 1 : 0);

                    for (int j = startIndex; j < endIndex; j++) {
                        String znodePath = znodePaths.get(j);
                        long startTime = System.nanoTime();
                        zooKeeper.delete(znodePath, -1);
                        long endTime = System.nanoTime();
                        long latency = endTime - startTime;
                        latencies.add(latency);
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
        
        logger.info("Total delete latency: " + totalLatency + " ns");
        logger.info("Average delete latency: " + avgLatency + " ns");
        logger.info("50th percentile delete latency: " + getPercentile(latencies, 50) + " ns");
        logger.info("90th percentile delete latency: " + getPercentile(latencies, 90) + " ns");
        logger.info("99.99th percentile delete latency: " + getPercentile(latencies, 99.99) + " ns");
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
}
