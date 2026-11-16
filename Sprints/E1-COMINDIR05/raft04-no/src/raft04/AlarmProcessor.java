package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AlarmProcessor {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }
        int count = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pub = new IpfsPubSub(api);
        HeartbeatMonitor[] monitors = new HeartbeatMonitor[count];
        for (int p = 0; p < count; p++) {
            final int part = p;
            monitors[p] = new HeartbeatMonitor(part, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, message -> System.out.println(message));
            new Thread(() -> {
                try {
                    pub.subscribe(Wire.topicData(part), message -> {
                        try {
                            if (!"LOTE".equals(Wire.type(message))) {
                                return;
                            }
                            Batch batch = Batch.decode(message, Integer.MAX_VALUE);
                            if (batch.term < 0) {
                                return;
                            }
                            monitors[part].onHeartbeat(batch.term, batch.leader, batch.hb);
                            for (SensorRecord record : batch.records) {
                                System.out.println("PROCESSADO;p" + part + ";seq=" + record.getSeq() + ";termo=" + record.getTerm());
                            }
                        } catch (Exception e) {
                            System.out.println("REJEITADO: " + message);
                        }
                    });
                } catch (Exception e) {
                    System.out.println("Subscricao interrompida: " + e.getMessage());
                }
            }, "alarm-processor-p" + p).start();
        }
        ScheduledExecutorService checks = Executors.newSingleThreadScheduledExecutor();
        checks.scheduleAtFixedRate(() -> Arrays.stream(monitors).forEach(HeartbeatMonitor::check), Wire.VERIFICACAO_MS,
                Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }
}
