package rep01;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        try {
            long lastSequence = file.lastSeq();
            Map<Long, SensorRecord> pending = new TreeMap<>();
            try (MulticastSocket socket = new MulticastSocket(null)) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress(PORT));
                InetAddress group = InetAddress.getByName(GROUP);
                socket.joinGroup(group);
                System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);

                byte[] buffer = new byte[1000];
                while (true) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    String line = new String(packet.getData(), packet.getOffset(),
                            packet.getLength(), StandardCharsets.UTF_8);
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
                            file.append(record);
                            lastSequence = record.getSeq();
                            System.out.println("Aplicado: " + record.toLine());
                            while (pending.containsKey(lastSequence + 1)) {
                                SensorRecord next = pending.remove(lastSequence + 1);
                                file.append(next);
                                lastSequence = next.getSeq();
                                System.out.println("Aplicado (estava retido): " + next.toLine());
                            }
                        }
                    } catch (IllegalArgumentException e) {
                        System.out.println("REJEITADO: " + line);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
