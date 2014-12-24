Samsa
=====

[![Build Status](https://travis-ci.org/bernd/samsa.svg)](https://travis-ci.org/bernd/samsa)

This is just me playing around with porting the [Apache Kafka](http://kafka.apache.org/)
log manager to pure Java.


## Status

Based on Apache Kafka commit [8452187](https://github.com/apache/kafka/commit/8452187).

The first step is done: a transliteration of the Scala code to Java to get a
working system as soon as possible. All tests pass!

Next up should be a compatibility test with Kafka to see if the logs can be
read by both log managers.

The code might have some weird constructs/namings/etc because of the 1-to-1
transliteration and can be refactored to be more Java-like.


## Usage

### Write & Read

```java
public class WriteReadTest {
    private static final Logger LOG = LoggerFactory.getLogger(WriteReadTest.class);

    public static void main(String[] args) throws Throwable {
        final int ioThreads = 2;
        final long flushCheckMs = TimeUnit.SECONDS.toMillis(60);
        final long flushCheckpointMs = TimeUnit.SECONDS.toMillis(60);
        final long retentionCheckMs = TimeUnit.SECONDS.toDays(20);

        final LogConfig logConfig = new LogConfigBuilder().build();

        final LogManager logManager = new LogManager(Lists.newArrayList(new File("/tmp/samsa-test")),
                new HashMap<String, LogConfig>(),
                logConfig,
                new CleanerConfigBuilder().build(),
                ioThreads, flushCheckMs, flushCheckpointMs, retentionCheckMs,
                new SamsaScheduler(2),
                new BrokerState(),
                new SystemTime());

        final TopicAndPartition topicAndPartition = new TopicAndPartition("test", 0);
        final Optional<Log> logOptional = logManager.getLog(topicAndPartition);
        final Log log;

        if (logOptional.isPresent()) {
            log = logOptional.get();
        } else {
            log = logManager.createLog(topicAndPartition, logConfig);
        }

        LOG.info("Initialized log at {}", logManager.getLogDirs());

        final LogAppendInfo info = log.append(new ByteBufferMessageSet(CompressionCodec.NONE, Lists.newArrayList(new Message("hello world".getBytes()))));

        LOG.info("Wrote message firstOffset={} lastOffset={} validBytes={}", info.firstOffset, info.lastOffset, info.validBytes);

        final FetchDataInfo fetchDataInfo = log.read(info.lastOffset, info.validBytes);
        final MessageSet messageSet = fetchDataInfo.getMessageSet();

        for (final MessageAndOffset messageAndOffset : messageSet) {
            final Message message = messageAndOffset.getMessage();
            final long offset = messageAndOffset.getOffset();

            LOG.info("Read message: \"{}\" (at {})", new String(Utils.readBytes(message.payload())), offset);
        }
    }
}
```


## TODO

### Compatibility Tests

* Check if a log written by Kafka can be read by Samsa.
* Check if a log written by Samsa can be read by Kafka.

### Exceptions

* Some methods are currently declaring 4 or more exceptions. Check if this can
  be simplified. (Only throwing a `SamsaException`?)

### Compression

* Implement the LZ4 codec.

### Dependencies

* The current code makes heavy use of [Guava](https://github.com/google/guava)
  for handling `Iterables`. Maybe that dependency can be removed by writing
  a helper class which implements those parts. (or extend the Iterable classes
  that are used)

### Cleanup

* Remove unused methods, variables, etc.


## Running Tests

This runs the unit tests.

### Testsuite

```shell
$ mvn clean test
```

### Log Stress Test

```shell
$ mvn exec:java -Dexec.mainClass="com.github.bernd.samsa.executable.StressTestLog" \
       -Dexec.classpathScope="test"
```

### Linear Write Speed Test

The following commands write 5GB of data in 50KB chunks.

Using the Samsa log.

```shell
$ mvn exec:java -Dexec.mainClass="com.github.bernd.samsa.executable.TestLinearWriteSpeed" \
       -Dexec.args="--bytes 5368709120 --size 51200 --files 1 --log" \
       -Dexec.classpathScope="test"
```

Using a Channel.

```shell
$ mvn exec:java -Dexec.mainClass="com.github.bernd.samsa.executable.TestLinearWriteSpeed" \
       -Dexec.args="--bytes 5368709120 --size 51200 --files 1 --channel" \
       -Dexec.classpathScope="test"
```

Using mmap'ed files.

```shell
$ mvn exec:java -Dexec.mainClass="com.github.bernd.samsa.executable.TestLinearWriteSpeed" \
       -Dexec.args="--bytes 5368709120 --size 51200 --files 10 --mmap" \
       -Dexec.classpathScope="test"
```
