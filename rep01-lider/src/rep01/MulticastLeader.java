package rep01;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner scanner = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1);
            RecordFile file = new RecordFile("lider.txt");
            long sequence = file.lastSeq();

            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | rajada <n> | sair");
            while (true) {
                System.out.print("> ");
                if (!scanner.hasNextLine()) {
                    break;
                }
                String input = scanner.nextLine().trim();
                if (input.equalsIgnoreCase("sair")) {
                    break;
                }
                if (input.toLowerCase().startsWith("rajada ")) {
                    String[] parts = input.split("\\s+");
                    try {
                        int count = Integer.parseInt(parts[1]);
                        for (int i = 0; i < count; i++) {
                            sequence = publish(socket, group, file, sequence,
                                    "SIM", 15 + RANDOM.nextDouble() * 15);
                        }
                        System.out.println("Rajada terminada: " + count + " registos");
                    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                        System.out.println("Formato inválido: rajada <n>");
                    }
                    continue;
                }

                String[] parts = input.split("\\s+");
                if (parts.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                try {
                    sequence = publish(socket, group, file, sequence, parts[0],
                            Double.parseDouble(parts[1]));
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + parts[1]);
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }

    private static long publish(MulticastSocket socket, InetAddress group,
                                RecordFile file, long sequence, String sensor,
                                double temperature) throws IOException {
        SensorRecord record = SensorRecord.now(++sequence, sensor, temperature);
        file.append(record);
        byte[] data = record.toLine().getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(data, data.length, group, PORT));
        System.out.println("Registado e enviado: " + record.toLine());
        return sequence;
    }
}
