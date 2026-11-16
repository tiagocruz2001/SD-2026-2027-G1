package raft04;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class Producer {
    private Producer() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        int count = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pub = new IpfsPubSub(api);
        Partitioner partitioner = new Partitioner(count);
        HeartbeatMonitor[] monitors = new HeartbeatMonitor[count];
        for (int p = 0; p < count; p++) {
            final int partition = p;
            monitors[p] = new HeartbeatMonitor(p, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    msg -> System.out.println(msg));
            Thread subscription = new Thread(() -> {
                try {
                    pub.subscribe(Wire.topicData(partition), message -> {
                        try {
                            if (!"LOTE".equals(Wire.type(message))) {
                                return;
                            }
                            String[] header = message.split("\\R", 2)[0].split(";", -1);
                            if (header.length != 9) {
                                throw new IllegalArgumentException("LOTE mal formado");
                            }
                            Batch batch = Batch.decode(message, Integer.parseInt(header[8]));
                            monitors[partition].onHeartbeat(batch.term, batch.leader, batch.hb);
                        } catch (RuntimeException e) {
                            System.out.println("LOTE rejeitado p" + partition + ": " + e.getMessage());
                        }
                    });
                } catch (Exception e) {
                    System.out.println("Subscricao interrompida p" + partition + ": " + e.getMessage());
                }
            }, "raft04-producer-p" + partition);
            subscription.setDaemon(true);
            subscription.start();
        }
        ScheduledExecutorService checks = Executors.newSingleThreadScheduledExecutor();
        checks.scheduleAtFixedRate(() -> Arrays.stream(monitors).forEach(m -> m.check()), Wire.VERIFICACAO_MS,
                Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        int idx = 0;
        while (true) {
            String sensor = "sensor" + idx;
            int p = partitioner.of(sensor);
            if (monitors[p].state() == HeartbeatMonitor.State.SUSPEITO) {
                System.out.println("AVISO: particao p" + p + " sem lider conhecido; leitura " + sensor + " pode perder-se");
            }
            String msg = Wire.in(sensor, idx * 1.5, idx, new VectorClock(Wire.GRUPO));
            pub.publish(Wire.topicIn(p), msg);
            Path file = Paths.get("produtor.txt");
            Files.writeString(file, System.currentTimeMillis() + ";" + msg + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            idx++;
            Thread.sleep(2000L);
        }
    }
}