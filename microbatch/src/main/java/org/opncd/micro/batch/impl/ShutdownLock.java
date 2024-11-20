package org.opncd.micro.batch.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shutdown lock to make sure all the active jobs are being fed into the batch processor and finished before
 * shutting down the thread pools.
 */
public class ShutdownLock {
    private static final Logger logger = LoggerFactory.getLogger(ShutdownLock.class);
    private final AtomicInteger activeJobCount;
    private final Semaphore runLock;

    /**
     * Create counter and the lock.
     */
    protected ShutdownLock() {
        this.activeJobCount = new AtomicInteger(0);
        this.runLock = new Semaphore(1);
    }

    /**
     * When a new job is added, this is used to increment active job count.
     *
     * @throws InterruptedException if failed to acquire lock
     */
    protected void incrementActiveJobCount() throws InterruptedException {
        logger.debug("Increment active job count");
        int newCount = this.activeJobCount.incrementAndGet();
        //if at least 1 job is submitted, acquire lock, so that system cannot shut down before finishing that 1 job
        if (newCount == 1) {
            this.runLock.acquire();
        }
    }

    /**
     * When jobs are processed, decrement the count back.
     *
     * @param count of jobs processed.
     */
    protected void decrementJobCount(int count) {
        logger.debug("Decrement active job count by: {}", count);
        int newCount = this.activeJobCount.addAndGet(-count);
        //if count reaches back to zero, then release shutdown lock.
        if (newCount == 0) {
            this.runLock.release();
        }
    }

    /**
     * Method to wait until all active jobs are processed.
     *
     * @throws InterruptedException if failed to acquire the lock.
     */
    protected void waitForShutdown() throws InterruptedException {
        logger.debug("Wait for active jobs to finish, current active job count: {}", this.activeJobCount.get());
        this.runLock.acquire();//trying to acquire the lock which won't be possible if there is at least one active job
    }
}
