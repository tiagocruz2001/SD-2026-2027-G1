package rep01;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) throws Exception {
        String api = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pubsub = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider.txt");
        long seq = file.lastSeq();
        try (Scanner sc = new Scanner(System.in)) {
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'rajada <n>' | 'sair'");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) {
                    break;
                }
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) {
                    break;
                }
                if (line.regionMatches(true, 0, "rajada ", 0, 7)) {
                    handleBurst(pubsub, file, line, seq);
                    seq = file.lastSeq();
                    continue;
                }
                String[] parts = line.split("\\s+");
                if (parts.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + parts[1]);
                    continue;
                }
                SensorRecord r = SensorRecord.now(++seq, parts[0], temp);
                file.append(r);
                pubsub.publish(TOPIC, r.toLine());
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }

    private static void handleBurst(IpfsPubSub pubsub, RecordFile file, String line, long seq) throws IOException, InterruptedException {
        String[] parts = line.split("\\s+");
        if (parts.length != 2) {
            System.out.println("Uso: rajada <n>");
            return;
        }
        int count;
        try {
            count = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            System.out.println("Número inválido: " + parts[1]);
            return;
        }
        if (count <= 0) {
            System.out.println("A rajada tem de ter pelo menos 1 registo.");
            return;
        }
        Random random = new Random();
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            SensorRecord r = SensorRecord.now(++seq, "SIM", 15 + random.nextDouble() * 15.0);
            file.append(r);
            pubsub.publish(TOPIC, r.toLine());
        }
        long end = System.currentTimeMillis();
        System.out.println("Rajada concluída: " + count + " registos enviados em " + (end - start) + " ms");
    }
}
