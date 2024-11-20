package org.opncd.micro.batch.api;

/**
 * Micro batch processor interface which provide required functionality to interact with the processor.
 *
 * @param <T> Job result generic type
 */
public interface MicroBatchProcessor<T> {

    /**
     * Method to submit a job to the micro batch processor, which returns the result wrapper to interact with the
     * final job result.
     *
     * @param job to be executed.
     * @return job result wrapper.
     * @throws Exception if failed to submit job
     */
    JobResult<T> submitJob(Job<T> job) throws Exception;

    /**
     * Method to shut down the processor gracefully, this will wait for any running jobs to be finished before
     * terminating the program, it will not accept any new jobs, and shutdown after finishing accepted jobs.
     *
     * @throws Exception if failed
     */
    void shutdown() throws Exception;
}
