package rep04;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Producer {
    public static void main(String[] args) {
        if (args.length < 1) { System.out.println("Uso: Producer <N> [api]"); return; }
        int count = Integer.parseInt(args[0]);
        IpfsPubSub pub = new IpfsPubSub(args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0");
        Partitioner partitioner = new Partitioner(count);
        HeartbeatMonitor[] monitors = new HeartbeatMonitor[count];
        PrintWriter log;
        try { log = new PrintWriter(new FileWriter("monitor-produtor.txt", true), true); }
        catch (IOException e) { System.out.println("Monitor: " + e.getMessage()); return; }
        for (int k = 0; k < count; k++) {
            final int p = k;
            monitors[p] = new HeartbeatMonitor(p, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    event(log));
            final HeartbeatMonitor monitor = monitors[p];
            new Thread(() -> {
                try {
                    pub.subscribe(Wire.topicData(p), message -> {
                        try {
                            String body = Wire.body(message);
                            String[] header = body.split("\\R", 2)[0].split(";", -1);
                            if (header.length != 7 || !"LOTE".equals(header[0]))
                                throw new IllegalArgumentException("LOTE invalido");
                            monitor.onHeartbeat(Long.parseLong(header[3]), Long.parseLong(header[4]));
                        } catch (RuntimeException e) {
                            System.out.println("LOTE rejeitado: " + e.getMessage());
                        }
                    });
                } catch (Exception e) { System.out.println("Subscricao p" + p + ": " + e.getMessage()); }
            }, "produtor-p" + p).start();
        }
        ScheduledExecutorService checks = Executors.newSingleThreadScheduledExecutor();
        checks.scheduleAtFixedRate(() -> Arrays.stream(monitors).forEach(HeartbeatMonitor::check),
                Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Produtor pronto. Formato: <sensor> <temperatura> | rajada <n> <s> | sair");
            while (scanner.hasNextLine()) {
                String[] parts = scanner.nextLine().trim().split("\\s+");
                if (parts.length == 0 || parts[0].isEmpty()) continue;
                if (parts[0].equalsIgnoreCase("sair")) break;
                try {
                    if (parts[0].equalsIgnoreCase("rajada")) {
                        int amount = Integer.parseInt(parts[1]), sensors = Integer.parseInt(parts[2]);
                        for (int i = 0; i < amount; i++) {
                            String sensor = String.format("S%02d", 1 + new Random().nextInt(sensors));
                            send(sensor, 15 + new Random().nextDouble() * 15, count, pub, partitioner, monitors);
                        }
                    } else if (parts.length == 2) {
                        send(parts[0], Double.parseDouble(parts[1]), count, pub, partitioner, monitors);
                    } else System.out.println("Formato invalido");
                } catch (Exception e) { System.out.println("Entrada rejeitada: " + e.getMessage()); }
            }
        } finally { checks.shutdownNow(); log.close(); }
    }

    private static void send(String sensor, double temperature, int count, IpfsPubSub pub,
            Partitioner partitioner, HeartbeatMonitor[] monitors) throws IOException, InterruptedException {
        int p = partitioner.of(sensor);
        if (monitors[p].state() == HeartbeatMonitor.State.SUSPEITO)
            System.out.println("AVISO: líder p" + p + " suspeito; leitura " + sensor + " pode perder-se");
        pub.publish(Wire.topicIn(p), Wire.in(sensor, temperature, 0, new VectorClock(count)));
    }

    private static java.util.function.Consumer<String> event(PrintWriter log) {
        return value -> { String line = System.currentTimeMillis() + ";" + value;
            System.out.println(line); log.println(line); };
    }
}
