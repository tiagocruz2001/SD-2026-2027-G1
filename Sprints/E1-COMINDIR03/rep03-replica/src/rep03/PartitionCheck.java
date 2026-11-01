package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PartitionCheck {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: PartitionCheck <N> <pasta>");
            return;
        }
        int count = Integer.parseInt(args[0]);
        Path folder = Paths.get(args[1]);
        Partitioner partitioner = new Partitioner(count);
        int total = 0;
        for (int k = 0; k < count; k++) {
            Path leader = folder.resolve("lider-p" + k + ".txt");
            List<SensorRecord> records = read(leader);
            Set<String> sensors = new HashSet<>();
            long expected = 1;
            for (SensorRecord r : records) {
                if (partitioner.of(r.getSensorId()) != k)
                    System.out.println("PARTICAO ERRADA p" + k + ": " + r.toLine());
                if (r.getSeq() != expected++)
                    System.out.println("SEQUENCIA INVALIDA p" + k + ": " + r.toLine());
                sensors.add(r.getSensorId());
            }
            total += records.size();
            int replicas = 0;
            for (int id = 1; id <= 3; id++) {
                Path replica = folder.resolve("replica-" + id + "-p" + k + ".txt");
                if (Files.exists(replica)) {
                    replicas++;
                    List<SensorRecord> copy = read(replica);
                    boolean prefix = copy.size() <= records.size() && isPrefix(copy, records);
                    System.out.println("p" + k + " replica " + id + ": " + (prefix ? "prefixo" : "DIVERGENTE")
                            + ", falta=" + Math.max(0, records.size() - copy.size()));
                }
            }
            System.out.println("p" + k + ": registos=" + records.size() + ", sensores=" + sensors.size() + ", replicas="
                    + replicas);
        }
        System.out.println("TOTAL: " + total);
    }

    private static List<SensorRecord> read(Path path) throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        if (!Files.exists(path))
            return result;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8))
            if (!line.isBlank())
                result.add(SensorRecord.fromLine(line));
        return result;
    }

    private static boolean isPrefix(List<SensorRecord> prefix, List<SensorRecord> records) {
        for (int i = 0; i < prefix.size(); i++)
            if (!prefix.get(i).toLine().equals(records.get(i).toLine()))
                return false;
        return true;
    }
}
