package rep01;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        TreeMap<Long, SensorRecord> waiting = new TreeMap<>();
        long[] last = new long[] { 0L };
        try {
            last[0] = file.lastSeq();
        } catch (IOException e) {
            System.out.println("IO ao ler último seq: " + e.getMessage());
        }
        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);
            byte[] buffer = new byte[1000];
            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p);
                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                try {
                    SensorRecord record = SensorRecord.fromLine(line);
                    if (record.getSeq() <= last[0] || waiting.containsKey(record.getSeq())) {
                        System.out.println("DUPLICADO: " + record.getSeq());
                        continue;
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
                    } else {
                        waiting.put(record.getSeq(), record);
                        System.out.println("EM ESPERA: " + record.getSeq() + " (falta o registo " + (last[0] + 1) + ")");
                    }
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
