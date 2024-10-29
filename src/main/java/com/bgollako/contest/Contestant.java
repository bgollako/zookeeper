package com.bgollako.contest;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;

import java.io.IOException;
import java.util.List;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import org.apache.log4j.Logger;

/**
 * Represents a contestant that interacts with ZooKeeper for distributed coordination.
 * This class manages ZooKeeper znodes for a contestant in a distributed system.
 */
public class Contestant implements Watcher {
    private static final Logger logger = Logger.getLogger(Contestant.class);

    /** The name of the contestant */
    private String name;
    /** The ZooKeeper server address */
    private String address;
    
    /** The parent path in ZooKeeper where contestant znodes will be created */
    private String parentPath;
    
    /** The ZooKeeper client instance */
    private ZooKeeper zooKeeper;
    
    /** The full path of this contestant's znode in ZooKeeper */
    private String znodePath;

    /** The random number generator for this contestant */
    private Random rand;

    /** The executor service for this contestant */
    private ExecutorService executor;

    /** The boolean flag to indicate if the contestant is the leader */
    private boolean isLeader;

    /**
     * Creates a new Contestant instance and establishes connection with ZooKeeper.
     * This constructor initializes the ZooKeeper connection, creates a parent znode if it doesn't exist,
     * and sets up a persistent watch on the parent path.
     *
     * @param address The ZooKeeper server address to connect to
     * @param parentPath The parent path where contestant znodes will be created
     * @throws Exception If connection to ZooKeeper fails or if there are issues creating the parent znode
     */
    Contestant(String address, String parentPath, ExecutorService executor, String name) {
        this.address = address;
        this.parentPath = parentPath;
        this.rand = new Random();
        this.executor = executor;
        this.name = name;
        this.isLeader = false;
    }

    /**
     * Starts the contestant by connecting to ZooKeeper, creating the parent znode, and setting up a watch.
     *
     * @throws Exception If connection to ZooKeeper fails or if there are issues creating the parent znode
     */
    void start() throws Exception {
        // Connect to ZooKeeper
        try {
            this.zooKeeper = new ZooKeeper(
                address, 
                3000, 
                null);
        } catch (IOException e) {
            logger.error("Failed to connect to ZooKeeper", e);
            throw new Exception("Failed to connect to ZooKeeper");
        }

        // Create parent znode
        try {
            this.createZnode(this.parentPath, CreateMode.PERSISTENT);
            logger.info("Parent znode created : " + this.parentPath);
        } catch (NodeExistsException e) {
            // Parent znode already exists
        }

        // Contest
        this.contest();
    }

    /**
     * Initiates the contest by creating an ephemeral sequential znode under the parent path.
     * The sequential nature ensures unique ordering among contestants.
     *
     * @throws Exception If there are issues creating the contestant znode
     */
    private void contest() throws Exception {
        this.znodePath = this.createZnode(this.parentPath + "/contestant", CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info("Contestant " + this.name + " created znode " + this.znodePath);
        this.checkLeader();
    }

    /**
     * Checks if this contestant is the leader by comparing its znode path with the smallest child of the parent path.
     * If this contestant's znode is the smallest, it is considered the leader and schedules its znode for deletion.
     * If not, it sets a watch on the parent znode to be notified when the current leader's znode is deleted.
     *
     * @throws Exception If there are issues getting the children of the parent znode or setting a watch
     */
    private void checkLeader() throws Exception {
        // Get children of parent znode
        List<String> children = this.zooKeeper.getChildren(this.parentPath, this);

        // Sort children
        Collections.sort(children);
        // Get the smallest child
        String smallest = children.get(0);
        String current = this.znodePath.substring(this.parentPath.length() + 1);
        if (smallest.equals(current)) {
            logger.info("Contestant " + this.name + " with znode " + this.znodePath + " is the leader");
            this.isLeader = true;
            this.scheduleNodeDeletion();
        } else {
            int parent = Collections.binarySearch(children, current)-1;
            String parentZnode = this.parentPath + "/" + children.get(parent);
            this.zooKeeper.exists(parentZnode, this);
        }        
    }

    private void scheduleNodeDeletion() {
        this.executor.submit(() -> {
            try {
                Thread.sleep(this.generateRandomNumber(5000, 10000));
                this.deleteZnode();
                this.isLeader = false;
                this.contest();                
            } catch (Exception e) {
                logger.error("Error during node deletion or contest restart", e);
            }
        });
    }

    /**
     * Deletes this contestant's znode from ZooKeeper.
     * This method is typically called when the contestant wants to withdraw from the contest
     * or clean up its resources.
     *
     * @return true if deletion was successful, false otherwise
     */
    private boolean deleteZnode() {
        try {
            this.zooKeeper.delete(this.znodePath, -1);
            return true;
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to delete znode", e);
            return false;
        }
    }

    /**
     * Creates a new znode in ZooKeeper with the specified path and creation mode.
     *
     * @param path The full path where the znode should be created
     * @param createMode The ZooKeeper creation mode (persistent, ephemeral, etc.)
     * @return The actual path of the created znode (may be different from input path for sequential nodes)
     * @throws Exception If there are issues creating the znode
     */
    private String createZnode(String path, CreateMode createMode) throws Exception {
        return this.zooKeeper.create(
            path, 
            new byte[0], 
            ZooDefs.Ids.OPEN_ACL_UNSAFE, 
            createMode
        );
    }

    /**
     * Processes ZooKeeper watch events for the parent path.
     * This method is called whenever there are changes to the children of the parent path.
     * It determines if this contestant is the winner by checking if its znode has the lowest
     * sequential number among all contestants.
     *
     * @param event The WatchedEvent object containing details about the event that triggered this callback
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted:
                try {
                    this.checkLeader();
                } catch (Exception e) {
                    logger.error("Error during leader check", e);
                }
                break;
            default:
                break;
        }
    }

    /**
     * Generates a random number between two specified numbers.
     *
     * @param min The minimum value of the range
     * @param max The maximum value of the range
     * @return A random number between min and max (inclusive)
     */
    private int generateRandomNumber(int min, int max) {
        return this.rand.nextInt((max - min) + 1) + min;
    }

    /**
     * Stops the contestant by closing the ZooKeeper connection.
     */
    public void stop() {
        try {
            this.zooKeeper.close();
        } catch (InterruptedException e) {
            logger.error("Error closing ZooKeeper connection", e);
        }
    }
}
