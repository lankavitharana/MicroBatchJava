package org.opncd.micro.batch.impl;

import org.opncd.micro.batch.api.JobResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Job result wrapper which can be used to interact with the final result of the job.
 *
 * @param <T> Job result generic type
 */
public class JobResultImpl<T> implements JobResult<T> {
    private static final Logger logger = LoggerFactory.getLogger(JobResultImpl.class);
    private volatile T resultValue = null;
    private final Semaphore lock;
    private final Lock consumerLock;
    private Consumer<T> consumer;

    /**
     * Constructor to initiate job result instance
     */
    public JobResultImpl() {
        this.lock = new Semaphore(0);
        this.consumerLock = new ReentrantLock();
    }

    /**
     * Method to set the job result to the wrapper.
     *
     * @param value job result.
     */
    public void setValue(T value) {
        //acquire consumer lock to make sure flow will not have raise conditions and ignore the asynchronous function
        //if any function is provided
        this.consumerLock.lock();
        try {
            this.resultValue = value;
            this.lock.release(); //releasing this will allow any waiting thread to access the final job result.
            //If any async function is there, execute that using job result as a parameter
            if (this.consumer != null) {
                try {
                    consumer.accept(value);
                } catch (Exception e) {
                    //catching and handling all the error here as there is no place to throw the exception back to
                    logger.error("Failed to run on complete, {}", e.getMessage(), e);
                }
            }
        } finally {
            this.consumerLock.unlock();
        }
    }

    @Override
    public T waitForResult() throws InterruptedException {
        this.lock.acquire(); //this will wait until batch processor sets the result value.
        return this.resultValue;
    }

    public T waitForResult(long timeout, TimeUnit unit) throws InterruptedException {
        boolean acquired = this.lock.tryAcquire(timeout, unit); //this will wait until batch processor sets the result value.
        if (!acquired) {
            throw new InterruptedException("Timeout exceeded without acquiring the lock");
        }
        return this.resultValue;
    }

    @Override
    public void onComplete(Consumer<T> consumer) {
        //acquire consumer lock to make sure flow will not have raise conditions and ignore the asynchronous function
        //if any function is provided
        this.consumerLock.lock();
        try {
            //invoke async function if the result value is already available
            if (this.resultValue != null) {
                try {
                    consumer.accept(this.resultValue);
                } catch (Exception e) {
                    //logging the exception
                    logger.error("Failed to run on complete, {}", e.getMessage(), e);
                    throw e; //throwing the error up the chain as in this scenario, there is a place for the error to go
                }
            } else {
                this.consumer = consumer;
            }
        } finally {
            this.consumerLock.unlock();
        }
    }
}
