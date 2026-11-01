package rep04;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class AlarmProcessor {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }
        int count = Integer.parseInt(args[0]);
        IpfsPubSub pub = new IpfsPubSub(args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0");
        Partitioner partitioner = new Partitioner(count);
        HeartbeatMonitor[] monitors = new HeartbeatMonitor[count];
        Set<Long>[] seen = new Set[count];
        PrintWriter log;
        try {
            log = new PrintWriter(new FileWriter("monitor-processador.txt", true), true);
        } catch (IOException e) {
            System.out.println("Monitor: " + e.getMessage());
            return;
        }
        for (int k = 0; k < count; k++) {
            final int p = k;
            seen[p] = Collections.synchronizedSet(new HashSet<>());
            monitors[p] = new HeartbeatMonitor(p, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, event(log));
            final HeartbeatMonitor monitor = monitors[p];
            new Thread(() -> {
                try {
                    pub.subscribe(Wire.topicData(p), message -> {
                        try {
                            String header = Wire.body(message).split("\\R", 2)[0];
                            String[] fields = header.split(";", -1);
                            if (fields.length != 7 || !"LOTE".equals(fields[0]))
                                throw new IllegalArgumentException("LOTE invalido");
                            Batch batch = Batch.decode(message, Integer.parseInt(fields[6]));
                            monitor.onHeartbeat(batch.inc, batch.hb);
                            for (SensorRecord record : batch.records) {
                                if (!seen[p].add(record.getSeq()) || record.getSensorId().startsWith("ALARM-")
                                        || record.getTemperature() < Wire.LIMIAR_ALARME)
                                    continue;
                                int target = partitioner.of("ALARM-" + record.getSensorId());
                                if (monitors[target].state() == HeartbeatMonitor.State.SUSPEITO)
                                    System.out.println("AVISO: líder p" + target
                                            + " suspeito; alarme " + record.getSensorId() + " pode perder-se");
                                pub.publish(Wire.topicIn(target),
                                        Wire.in("ALARM-" + record.getSensorId(),
                                                record.getTemperature(), record.getLamport(), record.getVector()));
                            }
                        } catch (Exception e) {
                            System.out.println("LOTE rejeitado: " + e.getMessage());
                        }
                    });
                } catch (Exception e) {
                    System.out.println("Subscricao p" + p + ": " + e.getMessage());
                }
            }, "processador-p" + p).start();
        }
        ScheduledExecutorService checks = Executors.newSingleThreadScheduledExecutor();
        checks.scheduleAtFixedRate(() -> Arrays.stream(monitors).forEach(HeartbeatMonitor::check),
                Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    private static java.util.function.Consumer<String> event(PrintWriter log) {
        return value -> {
            String line = System.currentTimeMillis() + ";" + value;
            System.out.println(line);
            log.println(line);
        };
    }
}
