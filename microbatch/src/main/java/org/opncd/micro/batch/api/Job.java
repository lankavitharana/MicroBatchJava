package org.opncd.micro.batch.api;

import org.opncd.micro.batch.impl.JobResultImpl;

/**
 * Abstract Job class with required API methods.
 *
 * @param <T> Job result generic type
 */
public abstract class Job<T> {

    // Job result wrapper holder
    private final JobResultImpl<T> jobResult;

    /**
     * Constructor to initiate an instance
     */
    public Job() {
        this.jobResult = new JobResultImpl<>();
    }

    /**
     * Set job result once processed, the batch processor implementation should call this method to set the final job
     * results once processing is done for each job.
     *
     * @param result Result of the processed job
     */
    public void setResult(T result) {
        this.jobResult.setValue(result);
    }

    /**
     * Get job results once job result is available.
     *
     * @return Job result
     */
    public JobResult<T> getResult() {
        return this.jobResult;
    }
}
