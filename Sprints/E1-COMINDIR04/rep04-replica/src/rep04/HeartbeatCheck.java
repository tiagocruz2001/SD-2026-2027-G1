package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;

public final class HeartbeatCheck {
    private static final class BatchStats {
        long count, empty, records, maxRecords, bytes;
        final List<Long> intervals = new ArrayList<>();
        long lastInstant;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Path folder = Paths.get(args[1]);
        Map<Integer, BatchStats> stats = new HashMap<>();
        for (int p = 0; p < n; p++) {
            BatchStats value = readHeartbeats(folder.resolve("hb-p" + p + ".txt"));
            stats.put(p, value);
            printBatchStats(p, value);
        }
        Map<Integer, List<Long>> failures = args.length > 2
                ? readFailures(folder.resolve(args[2]))
                : Collections.emptyMap();
        analyseMonitors(folder, stats, failures);
        printCausalSummary(folder, n);
    }

    private static BatchStats readHeartbeats(Path path) throws IOException {
        BatchStats result = new BatchStats();
        if (!Files.exists(path))
            return result;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank())
                continue;
            String[] fields = line.split(";", -1);
            if (fields.length < 6)
                continue;
            long instant = Long.parseLong(fields[2]);
            int records = Integer.parseInt(fields[4]);
            result.count++;
            result.records += records;
            result.maxRecords = Math.max(result.maxRecords, records);
            result.bytes += Long.parseLong(fields[5]);
            if (records == 0)
                result.empty++;
            if (result.lastInstant != 0)
                result.intervals.add(instant - result.lastInstant);
            result.lastInstant = instant;
        }
        return result;
    }

    private static void printBatchStats(int partition, BatchStats s) {
        double averageRecords = s.count == 0 ? 0 : (double) s.records / s.count;
        double averageInterval = s.intervals.stream().mapToLong(Long::longValue)
                .average().orElse(0);
        long maxInterval = s.intervals.stream().mapToLong(Long::longValue).max().orElse(0);
        double averageBytes = s.records == 0 ? 0 : (double) s.bytes / s.records;
        System.out.println("p" + partition + ": lotes=" + s.count
                + ";vazios=" + (s.count == 0 ? 0 : (double) s.empty / s.count)
                + ";registos_media=" + averageRecords + ";registos_max=" + s.maxRecords
                + ";intervalo_media_ms=" + averageInterval
                + ";intervalo_max_ms=" + maxInterval
                + ";bytes_registo_lote=" + averageBytes);
    }

    private static Map<Integer, List<Long>> readFailures(Path path) throws IOException {
        Map<Integer, List<Long>> result = new HashMap<>();
        if (!Files.exists(path))
            return result;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String[] fields = line.trim().split(";", -1);
            if (fields.length != 2 || !fields[0].startsWith("p"))
                continue;
            result.computeIfAbsent(Integer.parseInt(fields[0].substring(1)),
                    ignored -> new ArrayList<>()).add(Long.parseLong(fields[1]));
        }
        return result;
    }

    private static void analyseMonitors(Path folder, Map<Integer, BatchStats> stats,
            Map<Integer, List<Long>> failures) throws IOException {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(folder, "monitor-*.txt")) {
            for (Path file : files) {
                int suspected = 0, falseSuspected = 0, recovered = 0, lostHeartbeats = 0;
                Map<Integer, Long> pendingSuspicions = new HashMap<>();
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    String[] fields = line.split(";", -1);
                    if (fields.length < 3)
                        continue;
                    long instant;
                    try {
                        instant = Long.parseLong(fields[0]);
                    } catch (NumberFormatException e) {
                        continue;
                    }
                    int partition = partition(fields[2]);
                    if (line.contains(";SUSPEITA;")) {
                        suspected++;
                        pendingSuspicions.put(partition, instant);
                    } else if (line.contains(";RETOMA;")) {
                        if (pendingSuspicions.remove(partition) != null)
                            falseSuspected++;
                    } else if (line.contains(";RECUPERACAO;"))
                        recovered++;
                    else if (line.contains(";HB_PERDIDO;"))
                        lostHeartbeats++;
                }
                for (Map.Entry<Integer, Long> entry : pendingSuspicions.entrySet()) {
                    List<Long> starts = failures.getOrDefault(entry.getKey(), Collections.emptyList());
                    long closest = starts.stream().filter(value -> value <= entry.getValue())
                            .mapToLong(value -> entry.getValue() - value).min().orElse(-1);
                    System.out.println(file.getFileName() + ": p" + entry.getKey()
                            + " falha_real=true;deteccao_ms=" + closest);
                }
                System.out.println(file.getFileName() + ": suspeitas=" + suspected
                        + ";falsas=" + falseSuspected + ";recuperacoes=" + recovered
                        + ";hb_perdidos=" + lostHeartbeats);
            }
        }
    }

    private static int partition(String value) {
        int start = value.indexOf("p");
        if (start < 0)
            return -1;
        int end = start + 1;
        while (end < value.length() && Character.isDigit(value.charAt(end)))
            end++;
        return Integer.parseInt(value.substring(start + 1, end));
    }

    private static void printCausalSummary(Path folder, int n) {
        int valid = 0, invalid = 0;
        for (int p = 0; p < n; p++) {
            Path file = folder.resolve("lider-p" + p + ".txt");
            try {
                if (!Files.exists(file))
                    continue;
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.isBlank())
                        continue;
                    SensorRecord record = SensorRecord.fromLine(line);
                    if (record.getVector().get(p) == record.getSeq())
                        valid++;
                    else
                        invalid++;
                }
            } catch (Exception e) {
                System.out.println("CausalCheck indisponivel p" + p + ": " + e.getMessage());
            }
        }
        System.out.println("causalidade: V[k]==seq validos=" + valid + ";violacoes=" + invalid);
    }
}
