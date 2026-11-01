package rep03;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;

public class CausalCheck {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Uso: CausalCheck <N> <pasta>");
            return;
        }

        int count = Integer.parseInt(args[0]);
        Path folder = Paths.get(args[1]);
        List<Entry> entries = new ArrayList<>();

        for (int k = 0; k < count; k++) {
            Path file = folder.resolve("lider-p" + k + ".txt");

            if (!Files.exists(file)) {
                System.out.println("Ficheiro inexistente: " + file);
                continue;
            }

            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    SensorRecord record = SensorRecord.fromLine(line);
                    entries.add(new Entry(k, record));

                    if (record.getVector().get(k) != record.getSeq()) {
                        System.out.println("VIOLACAO V[" + k + "] != seq: " + line);
                    }
                }
            }
        }

        int before = 0;
        int after = 0;
        int concurrent = 0;
        int clockViolations = 0;
        int concurrentLamportDifferent = 0;

        for (int i = 0; i < entries.size(); i++) {
            for (int j = i + 1; j < entries.size(); j++) {
                Entry a = entries.get(i);
                Entry b = entries.get(j);

                if (a.partition == b.partition) {
                    continue;
                }

                VectorClock.Order order = a.record.getVector().compare(b.record.getVector());

                if (order == VectorClock.Order.BEFORE) {
                    before++;
                    if (a.record.getLamport() >= b.record.getLamport()) {
                        clockViolations++;
                    }
                } else if (order == VectorClock.Order.AFTER) {
                    after++;
                    if (b.record.getLamport() >= a.record.getLamport()) {
                        clockViolations++;
                    }
                } else if (order == VectorClock.Order.CONCURRENT) {
                    concurrent++;
                    if (a.record.getLamport() != b.record.getLamport()) {
                        concurrentLamportDifferent++;
                    }
                }
            }
        }

        System.out.println("a -> b: " + before);
        System.out.println("b -> a: " + after);
        System.out.println("concorrentes: " + concurrent);
        System.out.println("violacoes de Lamport: " + clockViolations);
        System.out.println("concorrentes com L distinto: " + concurrentLamportDifferent);

        double proportion = concurrent == 0 ? 0.0 : (double) concurrentLamportDifferent / concurrent;

        System.out.println("proporcao concorrentes com L distinto: " + proportion);

        System.out.println("Partições: " + count);

        entries.sort(Comparator
        .comparingLong((Entry e) -> e.record.getLamport())
        .thenComparingInt(e -> e.partition));

        Path view = folder.resolve("vista-lamport.txt");

        try (var writer = Files.newBufferedWriter(view, StandardCharsets.UTF_8)) {
            for (Entry entry : entries) {
                writer.write("p" + entry.partition + ";" + entry.record.toLine());
                writer.newLine();
            }
        }

        System.out.println("Pasta: " + folder);
    }

    private static final class Entry {
        final int partition;
        final SensorRecord record;

        Entry(int partition, SensorRecord record) {
            this.partition = partition;
            this.record = record;
        }
    }
}