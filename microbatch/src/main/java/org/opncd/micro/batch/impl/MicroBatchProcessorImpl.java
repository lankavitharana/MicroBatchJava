package org.opncd.micro.batch.impl;

import org.opncd.micro.batch.api.BatchProcessor;
import org.opncd.micro.batch.api.JobResult;
import org.opncd.micro.batch.api.MicroBatchProcessor;
import org.opncd.micro.batch.api.Job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Micro batch processor implementation, which support both interval based and size based batch configs,
 * implementation of a batch processor also can be injected to be used internally. Note that this implementation
 * does not guarantee the order of the jobs being processed, items may overlap, although you can use overloaded
 * constructor to initialize a thread pool of size 1, which will help run jobs sequentially.
 * this also accepts a failed notifier which will be notified if a batch of jobs failed, notifier will receive
 * the list of failed jobs. (micro batch processor cannot filter just the failed jobs in the batch as failure happens
 * in the downstream batch processor, hence it will provide the whole batch)
 *
 * @param <T> Job result generic type
 */
public class MicroBatchProcessorImpl<T> implements MicroBatchProcessor<T> {
    private static final Logger logger = LoggerFactory.getLogger(MicroBatchProcessorImpl.class);
    private final int batchSize;
    private List<Job<T>> jobList;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduler;
    private final Lock jobListLock;
    private volatile boolean acceptJobs;
    private final BatchProcessor<T> batchProcessor;
    private final ShutdownLock shutdownLock;
    private final BiConsumer<RuntimeException, List<Job<T>>> failedJobNotifier;

    /**
     * Micro batch processor constructor which accepts basic micro batch configs as well as a batch processor
     * implementation and also a notifier which can be notified if any or the batch jobs failed
     *
     * @param batchInterval     interval between each run.
     * @param intervalTimeUnit  interval time unit.
     * @param batchSize         batch size config.
     * @param batchProcessor    underlying batch processor to be used to run jobs.
     * @param failedJobNotifier can provide a notifier which will be notified when any batch of jobs failed.
     * @param executorPoolSize  underlying executor thread pool size.
     */
    public MicroBatchProcessorImpl(long batchInterval, TimeUnit intervalTimeUnit, int batchSize,
                                   BatchProcessor<T> batchProcessor,
                                   BiConsumer<RuntimeException, List<Job<T>>> failedJobNotifier, int executorPoolSize) {
        logger.debug("Initialising Micro Batch processor with parameters, batchInterval: {} and batchSize: {}",
                batchInterval, batchSize);
        this.batchSize = batchSize;
        this.batchProcessor = batchProcessor;
        this.jobListLock = new ReentrantLock();
        this.shutdownLock = new ShutdownLock();
        this.acceptJobs = true;
        this.executorService = Executors.newFixedThreadPool(executorPoolSize);
        this.failedJobNotifier = failedJobNotifier;
        this.jobList = new ArrayList<>();

        //for the current implementation, not allowing to configure underlying scheduler thread pool configs,
        //need to improve if required, for current requirements, pool size 1 is good enough
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        this.scheduleExecutor(batchInterval, intervalTimeUnit);
    }

    /**
     * Overloaded Micro batch processor constructor where underlying thread pool executor size has default value 4.
     *
     * @param batchInterval     interval between each run.
     * @param intervalTimeUnit  interval time unit.
     * @param batchSize         batch size config.
     * @param batchProcessor    underlying batch processor to be used to run jobs.
     * @param failedJobNotifier can provide a notifier which will be notified when any batch of jobs failed.
     */
    public MicroBatchProcessorImpl(long batchInterval, TimeUnit intervalTimeUnit, int batchSize,
                                   BatchProcessor<T> batchProcessor,
                                   BiConsumer<RuntimeException, List<Job<T>>> failedJobNotifier) {
        this(batchInterval, intervalTimeUnit, batchSize, batchProcessor, failedJobNotifier, 4);
    }

    /**
     * Method to initiate the scheduled task to run the batch processor periodically if jobs are available.
     *
     * @param batchInterval    interval between each run.
     * @param intervalTimeUnit interval time unit.
     */
    private void scheduleExecutor(long batchInterval, TimeUnit intervalTimeUnit) {
        logger.debug("Start scheduled task with parameters, batchInterval: {}", batchInterval);
        Runnable scheduledTask = () -> {
            try {
                List<Job<T>> currentJobItems = getRunnableJobs();

                //If no jobs to run, simply go to the next try by returning here
                if (currentJobItems == null) {
                    return;
                }
                //submit copied jobList to be executed in the batch processor to the thread pool, so that this thread is
                //not blocked while jobs are being executed, note that this submit happens outside the lock block because
                //now there is no need to keep the lock as we already copied the job items.
                this.executorService.submit(() -> runMicroBatch(currentJobItems));
            } catch (RuntimeException e) {
                //wrap and handle runtime exceptions so that pool will not crash even if some error is thrown
                logger.error("Error executing scheduled task, error: {}", e.getMessage(), e);
            }
        };

        this.scheduler.scheduleAtFixedRate(scheduledTask, batchInterval, batchInterval, intervalTimeUnit);
    }

    /**
     * Get runnable jobs that can be submitted to the batch processor from the queue. If no runnable jobs, return null
     *
     * @return runnable job list
     */
    private List<Job<T>> getRunnableJobs() {
        List<Job<T>> runnableJobs;
        try {
            //periodically checking if the job queue has values, if so process them, acquire a lock here so that
            //list is not updated while reading
            this.jobListLock.lock();
            //below logic needs to be inside the lock block so that if returned, the lock will be released
            if (this.jobList.isEmpty()) {
                logger.debug("No Jobs to be run in the queue, skipping to the next run");
                return null;
            }
            runnableJobs = new ArrayList<>(this.jobList); //copy current jobs to a new list
            logger.debug("Jobs available to process, count: {}", runnableJobs.size());
            this.jobList = new ArrayList<>(); //reset the global list so it can accept new jobs
        } finally {
            this.jobListLock.unlock();
        }
        return runnableJobs;
    }

    /**
     * Helper method to do the batch processing and once processed, decrement active job count.
     * if batch processor throws any errors, then this will notify the failed job notifier with failed jobs as well
     * as the error thrown by the batch processor.
     *
     * @param jobs to be batch processed
     */
    private void runMicroBatch(List<Job<T>> jobs) {
        logger.debug("Running batch processing on '{}' jobs", jobs.size());
        try {
            this.batchProcessor.processBatch(jobs);
        } catch (RuntimeException e) {
            //handle runtime errors here so that it won't break the thread pool.
            logger.error("Failed to run batch process, error: {}", e.getMessage(), e);
            //notify about failed jobs to the job submitter if he had provided a notifier
            notifyFailedJobs(e, jobs);
        } finally {
            //decrementing job count so that we can track how many jobs were submitted to the system and wait for them
            //to finish in the shutdown method
            this.shutdownLock.decrementJobCount(jobs.size());
        }
    }

    /**
     * This method is to notify about the failed jobs to the user by passing them back to the subscriber.
     *
     * @param e runtime exception
     * @param jobs failed jobs
     */
    private void notifyFailedJobs(RuntimeException e, List<Job<T>> jobs) {
        if (this.failedJobNotifier != null) {
            try {
                this.failedJobNotifier.accept(e, jobs);
            } catch (RuntimeException ex) {
                //again handling runtime errors of the notifier to make sure it won't break the thread pool
                logger.error("Failed to notify on failed jobs, error: {}", ex.getMessage(), ex);
                logger.error("Failed Job list details: {}", jobs.stream().map(Object::toString)
                        .collect(Collectors.joining(", ")));
            }
        } else {
            logger.error("Failed Job list details: {}", jobs.stream().map(Object::toString)
                    .collect(Collectors.joining(", ")));
        }
    }

    /**
     * This method will submit the job and if batch size is exceeded, submit the current job list to the
     * batch processor and return immediately with the result wrapper
     *
     * @param job to be processed
     * @return job result wrapper
     * @throws Exception if submit fails
     */
    @Override
    public JobResult<T> submitJob(Job<T> job) throws Exception {
        logger.debug("Submit job item");
        List<Job<T>> currentJobItems = submitJobToTheQueue(job);
        if (currentJobItems != null) {
            logger.debug("Jobs available to be run as a batch, count: {}", currentJobItems.size());
            //if there are jobs to run, run them in the thread pool, so it won't block the current thread and return
            //result wrapper to the invoker, this part does not need to be run within the lock block either
            this.executorService.submit(() -> runMicroBatch(currentJobItems));
        }
        return job.getResult();//return result wrapper
    }

    /**
     * Submit the job to the current job list and if the list size exceed batch size, return the jobs in the list and
     * cleanup the list for accepting more jobs.
     *
     * @param job to be submitted
     * @return current job list
     * @throws Exception if submit fails
     */
    private List<Job<T>> submitJobToTheQueue(Job<T> job) throws Exception {
        List<Job<T>> currentJobItems = null;
        try {
            //Locking the job list to avoid raise conditions.
            this.jobListLock.lock();
            if (!this.acceptJobs) { //means shutdown is triggered, so no longer accepting new jobs
                throw new Exception("Processor shutting down, no longer accepting jobs");
            }
            //add the new job to the global list
            this.jobList.add(job);
            //increment active job count
            this.shutdownLock.incrementActiveJobCount();
            //if the job list already reached batch size, then process that
            if (this.batchSize <= this.jobList.size()) {
                currentJobItems = new ArrayList<>(this.jobList); //copy current jobs to a new list
                logger.debug("Jobs available to process, count: {}", currentJobItems.size());
                this.jobList = new ArrayList<>(); //reset the global list so it can accept new jobs
            }
        } finally {
            this.jobListLock.unlock();
        }
        return currentJobItems;
    }

    /**
     * This job will wait for all the submitted jobs to be processed, then only it will shut down the tread pools
     * making it thread-safe by synchronized so that if shutdown is called multiple times, it won't hang
     *
     * @throws Exception if shutdown fails
     */
    @Override
    public synchronized void shutdown() throws Exception {
        if (!this.acceptJobs) {
            logger.warn("Shutdown already triggered");
            return;
        }
        this.acceptJobs = false;
        this.shutdownLock.waitForShutdown(); //wait to active jobs count to become zero
        this.executorService.shutdown(); //shutdown executor thread pool
        this.scheduler.shutdown();//shutdown scheduler thread pool
        boolean terminated = this.executorService.awaitTermination(5, TimeUnit.MINUTES);
        if (!terminated) {
            this.executorService.shutdownNow();
            throw new Exception("Executor service Timeout exceeded before termination");
        }
        terminated = this.scheduler.awaitTermination(5, TimeUnit.MINUTES);
        if (!terminated) {
            this.scheduler.shutdownNow();
            throw new Exception("Scheduler service Timeout exceeded before termination");
        }
    }
}
