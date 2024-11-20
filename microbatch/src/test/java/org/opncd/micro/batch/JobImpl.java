package org.opncd.micro.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opncd.micro.batch.api.Job;

/**
 * Dummy job implementation to test the library.
 */
public class JobImpl extends Job<Result> {
    private static final Logger logger = LoggerFactory.getLogger(JobImpl.class);

    private final int jobId;
    private final long simulatedSleepTimeInMillis;
    public JobImpl(int id, long simulatedSleepTimeInMillis) {
        this.jobId = id;
        this.simulatedSleepTimeInMillis = simulatedSleepTimeInMillis;
    }

    public void runJob() {
        logger.info("Running the job, id: {}", this.jobId);
        try {
            Thread.sleep(this.simulatedSleepTimeInMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public int getJobId() {
        return jobId;
    }
}
