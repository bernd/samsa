package com.github.bernd.samsa.cleaner;

public class CleanerConfigBuilder {
    public int numThreads;
    public long dedupeBufferSize;
    public double dedupeBufferLoadFactor;
    public int ioBuffersize;
    public int maxMessageSize;
    public double maxIoBytesPerSecond;
    public long backOffMs;
    public boolean enableCleaner;
    public String hashAlgorithm;

    public CleanerConfigBuilder() {
        numThreads = CleanerConfig.Defaults.NUM_THREADS;
        dedupeBufferSize = CleanerConfig.Defaults.DEDUPE_BUFFER_SIZE;
        dedupeBufferLoadFactor = CleanerConfig.Defaults.DEDUPE_BUFFER_LOAD_FACTOR;
        ioBuffersize = CleanerConfig.Defaults.IO_BUFFER_SIZE;
        maxMessageSize = CleanerConfig.Defaults.MAX_MESSAGE_SIZE;
        maxIoBytesPerSecond = CleanerConfig.Defaults.MAX_IO_BYTES_PER_SECOND;
        backOffMs = CleanerConfig.Defaults.BACK_OFF_MS;
        enableCleaner = CleanerConfig.Defaults.ENABLE_CLEANER;
        hashAlgorithm = CleanerConfig.Defaults.HASH_ALGORITHM;
    }

    public CleanerConfig build() {
        return new CleanerConfig(numThreads,
                dedupeBufferSize,
                dedupeBufferLoadFactor,
                ioBuffersize,
                maxMessageSize,
                maxIoBytesPerSecond,
                backOffMs,
                enableCleaner,
                hashAlgorithm);
    }

    public CleanerConfigBuilder numThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
    }

    public CleanerConfigBuilder dedupeBufferSize(long dedupeBufferSize) {
        this.dedupeBufferSize = dedupeBufferSize;
        return this;
    }

    public CleanerConfigBuilder dedupeBufferLoadFactor(double dedupeBufferLoadFactor) {
        this.dedupeBufferLoadFactor = dedupeBufferLoadFactor;
        return this;
    }

    public CleanerConfigBuilder ioBuffersize(int ioBuffersize) {
        this.ioBuffersize = ioBuffersize;
        return this;
    }

    public CleanerConfigBuilder maxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
        return this;
    }

    public CleanerConfigBuilder maxIoBytesPerSecond(double maxIoBytesPerSecond) {
        this.maxIoBytesPerSecond = maxIoBytesPerSecond;
        return this;
    }

    public CleanerConfigBuilder backOffMs(long backOffMs) {
        this.backOffMs = backOffMs;
        return this;
    }

    public CleanerConfigBuilder enableCleaner(boolean enableCleaner) {
        this.enableCleaner = enableCleaner;
        return this;
    }

    public CleanerConfigBuilder hashAlgorithm(String hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
        return this;
    }
}
