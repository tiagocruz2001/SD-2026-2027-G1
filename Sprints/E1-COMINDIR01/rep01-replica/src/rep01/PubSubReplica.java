package rep01;

import java.io.IOException;
import java.util.TreeMap;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) throws Exception {
        String id = args.length > 0 ? args[0] : "1";
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        IpfsPubSub pubsub = new IpfsPubSub(api);
        TreeMap<Long, SensorRecord> waiting = new TreeMap<>();
        long[] last = new long[] { file.lastSeq() };

        System.out.println("Réplica " + id + " à escuta do tópico " + TOPIC + " em " + api);
        pubsub.subscribe(TOPIC, line -> {
            try {
                SensorRecord record = SensorRecord.fromLine(line);
                if (record.getSeq() <= last[0] || waiting.containsKey(record.getSeq())) {
                    System.out.println("DUPLICADO: " + record.getSeq());
                    return;
                }
                if (record.getSeq() == last[0] + 1) {
                    file.append(record);
                    last[0] = record.getSeq();
                    System.out.println("APLICADO: " + record.toLine());
                    while (waiting.containsKey(last[0] + 1)) {
                        SensorRecord pending = waiting.remove(last[0] + 1);
                        file.append(pending);
                        last[0] = pending.getSeq();
                        System.out.println("APLICADO: " + pending.toLine());
                    }
                    return;
                }
                waiting.put(record.getSeq(), record);
                System.out.println("EM ESPERA: " + record.getSeq() + " (falta o registo " + (last[0] + 1) + ")");
            } catch (IllegalArgumentException e) {
                System.out.println("REJEITADO: " + line);
            } catch (IOException e) {
                System.out.println("IO ao escrever registo: " + e.getMessage());
            }
        });
    }
}
