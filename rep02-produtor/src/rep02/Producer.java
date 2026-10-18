package rep02;

import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class Producer {
    private static final Random RANDOM = new Random();
    public static void main(String[] args) {
        if (args.length < 1) { System.out.println("Uso: Producer <N> [api]"); return; }
        int count = Integer.parseInt(args[0]); String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pubSub = new IpfsPubSub(api); Partitioner partitioner = new Partitioner(count);
        try (Scanner scanner = new Scanner(System.in)) { System.out.println("Produtor pronto. Formato: <sensor> <temperatura> | rajada <n> <s> | rajada-quente <n> <s> | sair"); while (scanner.hasNextLine()) { String input = scanner.nextLine().trim(); if (input.equalsIgnoreCase("sair")) break; String[] parts = input.split("\\s+"); try {
            if (parts[0].equalsIgnoreCase("rajada") || parts[0].equalsIgnoreCase("rajada-quente")) { int n = Integer.parseInt(parts[1]); int sensors = Integer.parseInt(parts[2]); int[] sent = new int[count]; long first = System.currentTimeMillis(); for (int i = 0; i < n; i++) { String sensor = parts[0].equalsIgnoreCase("rajada-quente") && RANDOM.nextDouble() < .8 ? "S01" : "S" + String.format("%02d", parts[0].equalsIgnoreCase("rajada-quente") ? 2 + RANDOM.nextInt(Math.max(1, sensors - 1)) : 1 + RANDOM.nextInt(sensors)); double temperature = 15 + RANDOM.nextDouble() * 15; int k = partitioner.of(sensor); pubSub.publish(Wire.topicIn(k), Wire.in(sensor, temperature)); sent[k]++; } System.out.print("Rajada terminada: primeiro=" + first + "; ultimo=" + System.currentTimeMillis() + "; particoes="); for (int i = 0; i < count; i++) System.out.print((i == 0 ? "" : ",") + sent[i]); System.out.println(); continue; }
            if (parts.length != 2) { System.out.println("Formato invalido"); continue; } String sensor = parts[0]; double temperature = Double.parseDouble(parts[1]); int k = partitioner.of(sensor); pubSub.publish(Wire.topicIn(k), Wire.in(sensor, temperature)); System.out.println("Publicado " + sensor + " -> p" + k);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) { System.out.println("Formato invalido"); } catch (IOException e) { System.out.println("IO: " + e.getMessage()); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; } } }
    }
}
