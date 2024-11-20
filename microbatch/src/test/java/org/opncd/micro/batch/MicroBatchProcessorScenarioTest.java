package org.opncd.micro.batch;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opncd.micro.batch.api.Job;
import org.opncd.micro.batch.api.JobResult;
import org.opncd.micro.batch.api.MicroBatchProcessor;
import org.opncd.micro.batch.impl.MicroBatchProcessorImpl;

import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test micro batch processor. Note that these are mainly scenario testing (can categorize as Integration testing too)
 * even though it does test unit testing as well.
 */
public class MicroBatchProcessorScenarioTest {
    private static final Logger logger = LoggerFactory.getLogger(MicroBatchProcessorScenarioTest.class);

    /**
     * This tests returning results both ways, IE as async as well as waiting for result.
     *
     * @throws Exception failure
     */
    @Test
    void testReturnValues() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(1,
                TimeUnit.SECONDS, 5, new BatchProcessorImpl(), null);
        CompletableFuture<Result> future2 = new CompletableFuture<>();
        CompletableFuture<Result> future3 = new CompletableFuture<>();

        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 2));
        JobResult<Result> result2 = microBatchProcessor.submitJob(new JobImpl(2, 2));
        JobResult<Result> result3 = microBatchProcessor.submitJob(new JobImpl(3, 2));

        result2.onComplete(future2::complete);
        result3.onComplete(future3::complete);
        assertEquals(1, result1.waitForResult().getJobId(), "Unexpected job id found");
        assertEquals(2, future2.get().getJobId(), "Unexpected job id found");
        assertEquals(3, future3.get().getJobId(), "Unexpected job id found");

        microBatchProcessor.shutdown();
    }

    /**
     * This test the flow where batch processing happens because of the interval rather than batch size.
     *
     * @throws Exception failure
     */
    @Test
    void testBatchProcessingWithInterval() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(1,
                TimeUnit.SECONDS, 5, new BatchProcessorImpl(), null);
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 2));
        assertEquals(1, result1.waitForResult().getJobId(), "Unexpected job id found");
        microBatchProcessor.shutdown();
    }

    /**
     * This test the flow where batch processing happens because of the batch size rather than interval.
     *
     * @throws Exception failure
     */
    @Test
    void testBatchProcessingWithSize() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(1,
                TimeUnit.HOURS, 3, new BatchProcessorImpl(), null);
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 2));
        JobResult<Result> result2 = microBatchProcessor.submitJob(new JobImpl(2, 2));
        JobResult<Result> result3 = microBatchProcessor.submitJob(new JobImpl(3, 2));
        assertEquals(1, result1.waitForResult().getJobId(), "Unexpected job id found");
        assertEquals(2, result2.waitForResult().getJobId(), "Unexpected job id found");
        assertEquals(3, result3.waitForResult().getJobId(), "Unexpected job id found");
        microBatchProcessor.shutdown();
    }

    /**
     * This will test the flow where it skips one scheduled run before the jobs being executed.
     *
     * @throws Exception failure
     */
    @Test
    void testSkippingOneScheduledRun() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(10,
                TimeUnit.MILLISECONDS, 3, new BatchProcessorImpl(), null);
        Thread.sleep(20); //wait for first schedule to run
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 2));
        assertEquals(1, result1.waitForResult().getJobId(), "Unexpected job id found");
        microBatchProcessor.shutdown();
    }

    /**
     * Test where submitting jobs after initiating shutdown flow.
     *
     * @throws Exception failure
     */
    @Test
    void testSubmitAfterShutdownTriggered() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(2,
                TimeUnit.SECONDS, 3, new BatchProcessorImpl(), null);
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 2));
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                microBatchProcessor.shutdown();
            } catch (Exception e) {
                logger.error("Shutdown hook failed, error: {}", e.getMessage(), e);
            }
        });
        Thread.sleep(30); //wait until shutdown hook triggered
        assertThrows(Exception.class, () -> microBatchProcessor.submitJob(new JobImpl(2, 2)),
                "Expected exception to be thrown but it did not");
        assertEquals(1, result1.waitForResult().getJobId(), "Unexpected job id found");
        microBatchProcessor.shutdown();
    }

    /**
     * This test the scenario the micro batch processor can handle batch processor throwing runtime exceptions.
     *
     * @throws Exception failure
     */
    @Test
    void testBatchProcessorThrowingRuntimeException() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(10,
                TimeUnit.MILLISECONDS, 3, new BatchProcessorImpl(), null);
        JobResult<Result> result10 = microBatchProcessor.submitJob(new JobImpl(10, 1));
        Thread.sleep(30); //wait for runtime exception to happen

        JobResult<Result> result2 = microBatchProcessor.submitJob(new JobImpl(2, 2));

        assertEquals(2, result2.waitForResult().getJobId(), "Unexpected job id found");
        assertThrows(InterruptedException.class, () -> result10.waitForResult(30, TimeUnit.MILLISECONDS),
                "Expected interrupted exception");
        microBatchProcessor.shutdown();
    }

    /**
     * Test failure notifier being invoked if a batch of jobs failed.
     *
     * @throws Exception failure
     */
    @Test
    void testFailureNotifier() throws Exception {
        CompletableFuture<FailureWrapper> failureNotifier = new CompletableFuture<>();
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(10,
                TimeUnit.MILLISECONDS, 5, new BatchProcessorImpl(),
                (e, l) -> failureNotifier.complete(new FailureWrapper(e, l)));
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 1));
        assertEquals(1, result1.waitForResult().getJobId(), "Unexpected job id found");
        Thread.sleep(30); //wait for first batch to run
        JobResult<Result> result10 = microBatchProcessor.submitJob(new JobImpl(10, 1));
        Thread.sleep(30); //wait for runtime exception to happen
        JobResult<Result> result2 = microBatchProcessor.submitJob(new JobImpl(2, 2));

        assertEquals(2, result2.waitForResult().getJobId(), "Unexpected job id found");
        assertThrows(InterruptedException.class, () -> result10.waitForResult(30, TimeUnit.MILLISECONDS),
                "Expected interrupted exception");
        assertEquals(1, failureNotifier.get().getFailedJobs().size(), "Unexpected failure batch size");
        assertEquals(10, ((JobImpl) failureNotifier.get().getFailedJobs().get(0)).getJobId(),
                "Unexpected job id found");
        assertThrows(RuntimeException.class, () -> {
                    throw failureNotifier.get().getRuntimeException();
                },
                "Expected interrupted exception");
        microBatchProcessor.shutdown();
    }

    /**
     * This test the scenario where even the failure notifier throws an error, still the micro batch processor
     * is resilient and process rest of the jobs.
     *
     * @throws Exception failure
     */
    @Test
    void testFailureNotifierThrowingError() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(10,
                TimeUnit.MILLISECONDS, 5, new BatchProcessorImpl(), (e, l) -> {
            throw new RuntimeException("Failure notifier throwing runtime exception");
        });
        microBatchProcessor.submitJob(new JobImpl(10, 1));
        Thread.sleep(30); //wait for runtime exception to happen
        JobResult<Result> result2 = microBatchProcessor.submitJob(new JobImpl(2, 2));
        assertEquals(2, result2.waitForResult().getJobId(), "Unexpected job id found");
        microBatchProcessor.shutdown();

    }

    /**
     * Test where wait for result with timeout is successful.
     *
     * @throws Exception failure
     */
    @Test
    void testWaitForResultWithTimeoutSuccess() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(10,
                TimeUnit.MILLISECONDS, 5, new BatchProcessorImpl(), null);
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 2));
        assertEquals(1, result1.waitForResult(40, TimeUnit.MILLISECONDS).getJobId(),
                "Unexpected job id found");
        microBatchProcessor.shutdown();
    }

    /**
     * This test scenario where underlying batch processor is setting the result value, at the time, result consumer is
     * present, but it throws an exception.
     *
     * @throws Exception failure
     */
    @Test
    void testResultConsumerThrowsError() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(2,
                TimeUnit.MILLISECONDS, 5, new BatchProcessorImpl(), null);
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 30));
        result1.onComplete(r -> {
            throw new RuntimeException("Result consumer throwing runtime exception");
        });
        assertEquals(1, result1.waitForResult().getJobId(), "Unexpected job id found");

        JobResult<Result> result2 = microBatchProcessor.submitJob(new JobImpl(2, 5));
        assertEquals(2, result2.waitForResult().getJobId(), "Unexpected job id found");
        microBatchProcessor.shutdown();
    }

    /**
     * This test scenario where underlying batch processor has set the result value,
     * while the user sets the consumer, it throws and user has to handle that
     *
     * @throws Exception failure
     */
    @Test
    void testResultConsumerThrowsWhileSettingConsumer() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(2,
                TimeUnit.MILLISECONDS, 5, new BatchProcessorImpl(), null);
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 1));
        Thread.sleep(10); //wait until result value set so that when onComplete set, result is available
        assertThrows(RuntimeException.class, () -> result1.onComplete(r -> {
            throw new RuntimeException("Result consumer throwing runtime exception");
        }), "Expected Runtime exception to be thrown");

        assertEquals(1, result1.waitForResult().getJobId(), "Unexpected job id found");

        JobResult<Result> result2 = microBatchProcessor.submitJob(new JobImpl(2, 5));
        assertEquals(2, result2.waitForResult().getJobId(), "Unexpected job id found");
        microBatchProcessor.shutdown();
    }

    /**
     * This test scenario where underlying batch processor has set the result value,
     * while the user sets the consumer, it will get invoked with the result value
     *
     * @throws Exception failure
     */
    @Test
    void testResultConsumerWhileSettingConsumer() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(2,
                TimeUnit.MILLISECONDS, 5, new BatchProcessorImpl(), null);
        CompletableFuture<Result> future1 = new CompletableFuture<>();
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 1));
        Thread.sleep(10); //wait until result value set so that when onComplete set, result is available
        result1.onComplete(future1::complete);

        assertEquals(1, future1.get().getJobId(), "Unexpected job id found");

        microBatchProcessor.shutdown();
    }

    /**
     * This test the scenario where jobs are in the queue(not submitted to batch processor yet) but shutdown gets
     * triggered, still jobs in the queue needs to finish before shutdown returns
     *
     * @throws Exception failure
     */
    @Test
    void testJobsBeingProcessed() throws Exception {
        MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(200,
                TimeUnit.MILLISECONDS, 5, new BatchProcessorImpl(), null);
        JobResult<Result> result1 = microBatchProcessor.submitJob(new JobImpl(1, 1));
        microBatchProcessor.shutdown();
        assertEquals(1, result1.waitForResult().getJobId(), "Unexpected job id found");
    }

    private static class FailureWrapper {
        private final RuntimeException runtimeException;
        private final List<Job<Result>> failedJobs;

        FailureWrapper(RuntimeException runtimeException, List<Job<Result>> failedJobs) {
            this.runtimeException = runtimeException;
            this.failedJobs = failedJobs;
        }

        RuntimeException getRuntimeException() {
            return this.runtimeException;
        }

        List<Job<Result>> getFailedJobs() {
            return this.failedJobs;
        }
    }

}
