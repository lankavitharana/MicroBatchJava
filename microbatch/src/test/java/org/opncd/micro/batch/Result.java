package org.opncd.micro.batch;

/**
 * Dummy job result value implementation to test the library.
 */
public class Result {
    private final int jobId;

    public Result(int jobId) {
        this.jobId = jobId;
    }

    public int getJobId() {
        return jobId;
    }

}
