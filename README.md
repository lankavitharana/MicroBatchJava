# Micro Batch project

## Main points
1. Job processing order does not matter, (although you can achieve it by the library to some extent)
2. Each job has its own result and user needs to access it

## Notes
1. This system provides basic Micro batch layer library for usage as a library. It provides APIs for `Job`, 
`BatchProcessor` and `MicroBatchProcessor` so batchprocessor implementation should implement `Job` and `BatchProcessor` 
APIs, additionally, the library use java generics to make sure that the library user can plug in his own type of 
Job result for generic parameter T.
2. other than that, batchprocessor implementation needs to make sure that it will set the results of the job to the job 
object itself using provided abstract methods, this will make sure the result of the job will be propagated seamlessly
back to the user who submitted the job to the library.
3. micro batch processor accepts a notifier which will be notified if any of the batches of jobs failed, which can be
used to handle retry logics if required.
4. as future improvement, can add retry logic within the micro batch processor if required.
5. didn't use Factory pattern or anything to keep the library simple.
6. Using jdk version 8 for this project

## Gradle commands
Gradle is being used as the build tool for this project, following are the usefull gradle commands to run and test 
the application

### List gradle tasks
`./gradlew tasks`

### Build application
`./gradlew build`
this will generate following items
1. library jar file
2. javadocs for the library
3. test report
4. jacoco test coverage for the library

### Run tests
`./gradlew test`

### How to use the library
1. Import this to your project using any build system you like, example below for gradle
```dtd
dependencies {
    implementation 'org.opncd.micro.batch:microbatch:1.0.0'
}
```
2. Extend the `Job` abstract class and create your own job implementation\
you can refer the sample implementation in tests `org.opncd.micro.batch.JobImpl.java`
3. Implement your Job result (if it is simple value like String or Integer, you can use that as well)\
you can refer the sample implementation in tests `org.opncd.micro.batch.Result.java`
4. Implement Batch processor interface with custom batch processor logic(just make sure you set the job results for each job)\
you can refer the sample implementation in tests `org.opncd.micro.batch.BatchProcessorImpl.java`
5. Instantiate `MicrobatchProcessor` with required parameters, example as follows
```dtd
MicroBatchProcessor<Result> microBatchProcessor = new MicroBatchProcessorImpl<>(1, TimeUnit.SECONDS, 5, new BatchProcessorImpl(), null);
```
6. Submit jobs as follows
```dtd
JobResult<Result> jobResult = microBatchProcessor.submitJob(new JobImpl(1, 2));
```
7. You can get job result by either of following approaches
wait for the job result
```dtd
Result result = jobResult.waitForResult();
```
subscribe to the job result by providing consumer function
```dtd
jobResult.onComplete(r -> {
    System.out.println(r.getJobId());
});
```
8. Shutdown the micro batch processor (better to do that within try finally block to make sure resources being cleaned up)
```dtd
microBatchProcessor.shutdown();
```

### Build the project inside docker
you can use following generic command with relevant gradle task to build the project inside a docker container\
`docker run --rm -u gradle -v "$PWD":/home/gradle/project -w /home/gradle/project gradle:8.10.2-jdk8 gradle <gradle-task>`\
below is the example where it "build" the project\
`docker run --rm -u gradle -v "$PWD":/home/gradle/project -w /home/gradle/project gradle:8.10.2-jdk8 gradle build`
