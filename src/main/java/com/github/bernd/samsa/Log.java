package com.github.bernd.samsa;

import java.io.File;
import java.text.NumberFormat;

public class Log {
    /** a log file */
    public static String LOG_FILE_SUFFIX = ".log";

    /** an index file */
    public static String INDEX_FILE_SUFFIX = ".index";

    /** a file that is scheduled to be deleted */
    public static String DELETED_FILE_SUFFIX = ".deleted";

    /** A temporary file that is being used for log cleaning */
    public static String CLEANED_FILE_SUFFIX = ".cleaned";

    /** A temporary file used when swapping files into the log */
    public static String SWAP_FILE_SUFFIX = ".swap";

    /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
     * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
    /** TODO: Get rid of CleanShutdownFile in 0.8.2 */
    public static String CLEAN_SHUTDOWN_FILE = ".kafka_cleanshutdown";

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     * @param offset The offset to use in the file name
     * @return The filename
     */
    public static String filenamePrefixFromOffset(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * Construct a log file name in the given dir with the given base offset
     * @param dir The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File logFilename(final File dir, final long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + LOG_FILE_SUFFIX);
    }

    /**
     * Construct an index file name in the given dir using the given base offset
     * @param dir The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File indexFilename(final File dir, final long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + INDEX_FILE_SUFFIX);
    }
}
