package rep01;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String port = args.length > 1 ? args[1] : "5001";
        String api = port.startsWith("http")
                ? port
                : "http://127.0.0.1:" + port + "/api/v0";
        RecordFile file = new RecordFile("replica-" + id + ".txt");

        try {
            ReplicaState state = new ReplicaState(file);
            System.out.println("Réplica " + id + " a subscrever " + TOPIC + " em " + api);
            new IpfsPubSub(api).subscribe(TOPIC, state::apply);
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Subscrição interrompida.");
        }
    }

    private static final class ReplicaState {
        private final RecordFile file;
        private final Map<Long, SensorRecord> pending = new TreeMap<>();
        private long lastSequence;

        private ReplicaState(RecordFile file) throws IOException {
            this.file = file;
            this.lastSequence = file.lastSeq();
        }

        private synchronized void apply(String line) {
            try {
                SensorRecord record = SensorRecord.fromLine(line);
                long sequence = record.getSeq();
                if (sequence <= lastSequence || pending.containsKey(sequence)) {
                    System.out.println("DUPLICADO: " + sequence);
                } else if (sequence > lastSequence + 1) {
                    pending.put(sequence, record);
                    System.out.println("EM ESPERA: " + sequence
                            + " (falta o registo " + (lastSequence + 1) + ")");
                } else {
                    append(record);
                    while (pending.containsKey(lastSequence + 1)) {
                        append(pending.remove(lastSequence + 1));
                    }
                }
            } catch (IllegalArgumentException e) {
                System.out.println("REJEITADO: " + line);
            }
        }

        private void append(SensorRecord record) {
            try {
                file.append(record);
                lastSequence = record.getSeq();
                System.out.println("Aplicado: " + record.toLine());
            } catch (IOException e) {
                System.out.println("IO ao escrever " + record.getSeq() + ": " + e.getMessage());
            }
        }
    }
}
