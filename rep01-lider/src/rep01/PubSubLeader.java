package rep01;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        String api = args.length > 0
                ? "http://127.0.0.1:" + args[0] + "/api/v0"
                : "http://127.0.0.1:5001/api/v0";
        if (args.length > 0 && args[0].startsWith("http")) {
            api = args[0];
        }

        IpfsPubSub pubSub = new IpfsPubSub(api);
        try (Scanner scanner = new Scanner(System.in)) {
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
                        long first = System.currentTimeMillis();
                        for (int i = 0; i < count; i++) {
                            sequence = publish(pubSub, file, sequence, "SIM",
                                    15 + RANDOM.nextDouble() * 15);
                        }
                        long last = System.currentTimeMillis();
                        System.out.println("Rajada terminada: " + count
                                + " registos; primeiro=" + first + "; último=" + last);
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
                    sequence = publish(pubSub, file, sequence, parts[0],
                            Double.parseDouble(parts[1]));
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + parts[1]);
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }

    private static long publish(IpfsPubSub pubSub, RecordFile file, long sequence,
                                String sensor, double temperature)
            throws IOException {
        SensorRecord record = SensorRecord.now(++sequence, sensor, temperature);
        file.append(record);
        try {
            pubSub.publish(TOPIC, record.toLine());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("publicação interrompida", e);
        }
        System.out.println("Registado e publicado: " + record.toLine());
        return sequence;
    }
}
