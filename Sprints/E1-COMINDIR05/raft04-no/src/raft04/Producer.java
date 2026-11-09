package raft04;

import java.io.IOException;
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
        if (args.length < 2) {
            System.out.println("Uso: Producer <N> <api>");
            return;
        }
        int count = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pub = new IpfsPubSub(api);
        HeartbeatMonitor[] monitors = new HeartbeatMonitor[count];
        for (int p = 0; p < count; p++) {
            monitors[p] = new HeartbeatMonitor(p, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, msg -> System.out.println(msg));
        }
        ScheduledExecutorService checks = Executors.newSingleThreadScheduledExecutor();
        checks.scheduleAtFixedRate(() -> Arrays.stream(monitors).forEach(m -> m.check()), Wire.VERIFICACAO_MS,
                Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        int idx = 0;
        while (true) {
            int p = idx % count;
            if (monitors[p].state() == HeartbeatMonitor.State.SUSPEITO) {
                System.out.println("AVISO: particao p" + p + " sem lider conhecido; leitura sensor" + idx + " pode perder-se");
            }
            String sensor = "sensor" + idx;
            String msg = "IN;" + sensor + ";" + (idx * 1.5) + ";" + System.currentTimeMillis() + ";" + idx;
            pub.publish(Wire.topicIn(p), msg);
            Path file = Paths.get("produtor.txt");
            Files.writeString(file, System.currentTimeMillis() + ";" + msg + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            idx++;
            Thread.sleep(2000L);
        }
    }
}
