package com.github.bernd.samsa;

import com.google.common.collect.Maps;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * This class saves out a map of topic/partition=>offsets to a file.
 * <p/>
 * This class is thread-safe!
 */
public class OffsetCheckpoint {
    private final File file;
    private final Object lock = new Object();

    public OffsetCheckpoint(final File file) throws IOException {
        this.file = file;

        new File(file + ".tmp").delete(); // try to delete any existing temp files for cleanliness
        file.createNewFile(); // in case the file doesn't exist
    }

    public void write(final Map<TopicAndPartition, Long> offsets) throws IOException {
        synchronized (lock) {
            // write to temp file and then swap with the existing file
            final File temp = new File(file.getAbsolutePath() + ".tmp");

            final FileOutputStream fileOutputStream = new FileOutputStream(temp);
            final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
            try {
                // write the current version
                writer.write(String.valueOf(0));
                writer.newLine();

                // write the number of entries
                writer.write(String.valueOf(offsets.size()));
                writer.newLine();

                // write the entries
                for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet()) {
                    final TopicAndPartition topicPart = entry.getKey();
                    final Long offset = entry.getValue();

                    writer.write(String.format("%s %d %d", topicPart.getTopic(), topicPart.getPartition(), offset));
                    writer.newLine();
                }

                // flush the buffer and then fsync the underlying file
                writer.flush();
                fileOutputStream.getFD().sync();
            } finally {
                writer.close();
            }

            // swap new offset checkpoint file with previous one
            if (!temp.renameTo(file)) {
                // renameTo() fails on Windows if the destination file exists.
                file.delete();
                if (!temp.renameTo(file)) {
                    throw new IOException(String.format("File rename from %s to %s failed.", temp.getAbsolutePath(), file.getAbsolutePath()));
                }
            }
        }
    }

    public Map<TopicAndPartition, Long> read() throws IOException {
        synchronized (lock) {
            final BufferedReader reader = new BufferedReader(new FileReader(file));
            try {
                String line = reader.readLine();
                if (line == null) {
                    return Maps.newHashMap();
                }
                final int version = Integer.parseInt(line);
                switch (version) {
                    case 0:
                        line = reader.readLine();
                        if (line == null) {
                            return Maps.newHashMap();
                        }
                        int expectedSize = Integer.parseInt(line);
                        final Map<TopicAndPartition, Long> offsets = new HashMap<>();
                        line = reader.readLine();
                        while (line != null) {
                            final String[] pieces = line.split("\\s+");
                            if (pieces.length != 3) {
                                throw new IOException(String.format("Malformed line in offset checkpoint file: '%s'.", line));
                            }

                            final String topic = pieces[0];
                            final int partition = Integer.parseInt(pieces[1]);
                            final long offset = Long.parseLong(pieces[2]);
                            offsets.put(new TopicAndPartition(topic, partition), offset);
                            line = reader.readLine();
                        }
                        if (offsets.size() != expectedSize) {
                            throw new IOException(String.format("Expected %d entries but found only %d", expectedSize, offsets.size()));
                        }
                        return offsets;
                    default:
                        throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version);
                }
            } finally {
                reader.close();
            }
        }
    }
}
