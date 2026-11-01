package rep03;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class Producer {
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        int count = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pubSub = new IpfsPubSub(api);
        Partitioner partitioner = new Partitioner(count);

        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Produtor pronto. Formato: <sensor> <temperatura>"
                    + " | rajada <n> <s> | rajada-quente <n> <s> | sair");
            while (scanner.hasNextLine()) {
                String input = scanner.nextLine().trim();
                if (input.equalsIgnoreCase("sair")) {
                    break;
                }
                String[] parts = input.split("\\s+");
                try {
                    if (parts[0].equalsIgnoreCase("rajada")
                            || parts[0].equalsIgnoreCase("rajada-quente")) {
                        publishBurst(parts, count, pubSub, partitioner);
                        continue;
                    }
                    if (parts.length != 2) {
                        System.out.println("Formato invalido");
                        continue;
                    }
                    String sensor = parts[0];
                    double temperature = Double.parseDouble(parts[1]);
                    long lamport = 0;
                    VectorClock vector = new VectorClock(count);
                    int partition = partitioner.of(sensor);
                    pubSub.publish(Wire.topicIn(partition), Wire.in(sensor, temperature, lamport, vector));
                    System.out.println("Publicado " + sensor + " -> p" + partition);
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    System.out.println("Formato invalido");
                } catch (IOException e) {
                    System.out.println("IO: " + e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void publishBurst(String[] parts, int count, IpfsPubSub pubSub,
            Partitioner partitioner)
            throws IOException, InterruptedException {
        int amount = Integer.parseInt(parts[1]);
        int sensors = Integer.parseInt(parts[2]);
        int[] sent = new int[count];
        boolean hot = parts[0].equalsIgnoreCase("rajada-quente");
        long first = System.currentTimeMillis();

        for (int i = 0; i < amount; i++) {
            String sensor;
            if (hot && RANDOM.nextDouble() < .8) {
                sensor = "S01";
            } else {
                int sensorNumber = hot
                        ? 2 + RANDOM.nextInt(Math.max(1, sensors - 1))
                        : 1 + RANDOM.nextInt(sensors);
                sensor = String.format("S%02d", sensorNumber);
            }
            double temperature = 15 + RANDOM.nextDouble() * 15;
            long lamport = 0;
            VectorClock vector = new VectorClock(count);
            int partition = partitioner.of(sensor);
            pubSub.publish(Wire.topicIn(partition), Wire.in(sensor, temperature, lamport, vector));
            sent[partition]++;
        }

        System.out.print("Rajada terminada: primeiro=" + first
                + "; ultimo=" + System.currentTimeMillis() + "; particoes=");
        for (int i = 0; i < count; i++) {
            System.out.print((i == 0 ? "" : ",") + sent[i]);
        }
        System.out.println();
    }
}
