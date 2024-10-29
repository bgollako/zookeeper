package com.bgollako.contest;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;

/**
 * Represents a group of contestants that interact with ZooKeeper for distributed coordination.
 * This class manages ZooKeeper znodes for a group of contestants in a distributed system.
 */
public class Contestants {
    private static final Logger logger = Logger.getLogger(Contestants.class);

    /** The list of contestants */
    private List<Contestant> contestants;
    
    /** The number of contestants in the group */
    private int numberOfContestants;
    
    /** The executor service for the group of contestants */
    private ExecutorService executor;
    
    /** The ZooKeeper server address */
    private String zooKeeperAddress;
    
    /** The parent path in ZooKeeper where contestant znodes will be created */
    private String parentPath;

    /**
     * Creates a new Contestants instance and initializes the group of contestants.
     *
     * @param zooKeeperAddress The ZooKeeper server address to connect to
     * @param parentPath The parent path where contestant znodes will be created
     * @param noOfContestants The number of contestants in the group
     */
    public Contestants(String zooKeeperAddress, String parentPath, int noOfContestants) {
        this.contestants = new ArrayList<>();
        this.numberOfContestants = noOfContestants;
        this.zooKeeperAddress = zooKeeperAddress;
        this.parentPath = parentPath;
        this.executor = Executors.newFixedThreadPool(noOfContestants);
    }

    /**
     * Starts the group of contestants by creating and initializing each contestant.
     *
     * @throws Exception If there are issues creating or initializing the contestants
     */
    public void start() throws Exception {
        for (int i = 0; i < this.numberOfContestants; i++) {
            Contestant contestant = new Contestant(
                this.zooKeeperAddress, 
                this.parentPath, 
                this.executor,
                "Contestant " + i
            );
            this.executor.submit(()->{
                try {
                    contestant.start();
                } catch (Exception e) {
                    logger.error("Error starting contestant", e);
                }
            });
            this.contestants.add(contestant);
        }
    }

    /**
     * Stops the group of contestants by closing the executor service.
     */
    public void stop() {
        for (Contestant contestant : this.contestants) {
            contestant.stop();
        }
        this.executor.shutdown();
    }
}
