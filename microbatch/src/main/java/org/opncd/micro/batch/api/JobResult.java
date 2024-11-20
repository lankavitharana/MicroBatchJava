package org.opncd.micro.batch.api;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Job result interface which allows the user to interact with the result of the job. There are two options to
 * interact with the job result, which are as follows.
 * 1. wait for the result to return and access that.
 * 2. provide a method to be invoked when results are available asynchronously.
 *
 * @param <T> Job result generic type
 */
public interface JobResult<T> {
    /**
     * Method to wait for the result to return and access it. Note: This wait indefinitely until thread gets
     * interrupted or a result is returned.
     *
     * @return job result.
     * @throws InterruptedException if failed with the lock
     */
    T waitForResult() throws InterruptedException;

    /**
     * Method to wait for the result to return and access it.
     *
     * @param timeout time to wait
     * @param unit time unit
     * @return job result
     * @throws InterruptedException if failed with the lock
     */
    T waitForResult(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Method to provide asynchronous function to be invoked when results are available. This only support
     * one consumer function at the time, if required can improve to support multiple consumers as well.
     *
     * @param consumer asynchronous function.
     */
    void onComplete(Consumer<T> consumer);
}
