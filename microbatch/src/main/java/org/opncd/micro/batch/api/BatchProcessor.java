package org.opncd.micro.batch.api;

import java.util.List;

/**
 * Batch processor interface which needs to be implemented by the underlying batch processor implementation.
 *
 * @param <T> Job result generic type
 */
public interface BatchProcessor<T> {

    /**
     * Process batch jobs API which needs to be implemented by the batch processor implementation.
     *
     * @param jobList job list to be processed
     */
    void processBatch(List<Job<T>> jobList);

}
