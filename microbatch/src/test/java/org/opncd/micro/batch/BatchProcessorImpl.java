package org.opncd.micro.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opncd.micro.batch.api.BatchProcessor;
import org.opncd.micro.batch.api.Job;

import java.util.List;

/**
 * Batch processor dummy implementation to test the library
 */
public class BatchProcessorImpl implements BatchProcessor<Result> {
    private static final Logger logger = LoggerFactory.getLogger(BatchProcessorImpl.class);

    @Override
    public void processBatch(List<Job<Result>> jobList) {
        logger.info("Processing jobs in batch processor, number of jobs {}", jobList.size());
        for (Job<Result> job : jobList) {
            JobImpl jobImpl = (JobImpl) job;
            jobImpl.runJob();
            if (jobImpl.getJobId() == 10) {
                throw new RuntimeException("Throw exception for job id: " + jobImpl.getJobId());
            }
            Result result = new Result(jobImpl.getJobId());
            jobImpl.setResult(result); //Setting the job result, this is important for the job result to be propagated back to the caller
        }
    }
}
