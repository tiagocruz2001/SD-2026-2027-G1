package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class RecordFile {
    private final Path path;
    private final int partitionCount;

    public RecordFile(String fileName) {
        this(fileName, 3);
    }

    public RecordFile(String fileName, int partitionCount) {
        this.path = Paths.get(fileName);
        this.partitionCount = partitionCount;
    }

    public synchronized void append(SensorRecord record) throws IOException {
        Files.writeString(path, record.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized long lastSeq() throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        long last = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line, partitionCount).getSeq();
            }
        }
        return last;
    }

    public synchronized long lastTerm() throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        long last = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line, partitionCount).getTerm();
            }
        }
        return last;
    }

    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        if (!Files.exists(path)) {
            return result;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                SensorRecord record = SensorRecord.fromLine(line, partitionCount);
                if (record.getSeq() >= from && record.getSeq() <= to) {
                    result.add(record);
                }
            }
        }
        return result;
    }

    public synchronized SensorRecord last() throws IOException {
        if (!Files.exists(path)) {
            return null;
        }
        SensorRecord last = null;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line, partitionCount);
            }
        }
        return last;
    }

    public synchronized List<SensorRecord> readAll() throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        if (!Files.exists(path)) {
            return result;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                result.add(SensorRecord.fromLine(line, partitionCount));
            }
        }
        return result;
    }

    public synchronized void replaceAll(List<SensorRecord> records) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        StringBuilder content = new StringBuilder();
        for (SensorRecord record : records) {
            content.append(record.toLine()).append(System.lineSeparator());
        }
        Files.writeString(tmp, content.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
