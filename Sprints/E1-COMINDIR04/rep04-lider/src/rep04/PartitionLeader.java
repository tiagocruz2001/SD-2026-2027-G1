package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class PartitionLeader {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereco] [api]");
            return;
        }
        int partition = Integer.parseInt(args[0]), count = Integer.parseInt(args[1]);
        String host = args.length > 2 ? args[2] : "127.0.0.1",
                api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";
        if (partition < 0 || partition >= count) {
            System.out.println("Particao invalida");
            return;
        }
        RecordFile file = new RecordFile("lider-p" + partition + ".txt");
        Partitioner part = new Partitioner(count);
        IpfsPubSub pub = new IpfsPubSub(api);
        int port = Wire.PORTO_BASE + partition;
        Object lock = new Object();
        long[] seq = { 0 }, lamport = { 0 }, inc = { System.currentTimeMillis() }, hb = { 0 };
        VectorClock[] vector = { new VectorClock(count) };
        List<SensorRecord> pending = new ArrayList<>();
        try {
            SensorRecord last = file.last();
            if (last != null) {
                seq[0] = last.getSeq();
                lamport[0] = last.getLamport();
                vector[0] = last.getVector();
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
            return;
        }
        Thread input = new Thread(() -> {
            try {
                pub.subscribe(Wire.topicIn(partition), message -> {
                    try {
                        if (!"IN".equals(Wire.type(message)))
                            throw new IllegalArgumentException();
                        String[] f = Wire.body(message).split(";", -1);
                        if (f.length != 4)
                            throw new IllegalArgumentException();
                        String sensor = f[0];
                        double temp = Double.parseDouble(f[1]);
                        long lm = Long.parseLong(f[2]);
                        VectorClock vm = VectorClock.parse(f[3], count);
                        if (part.of(sensor) != partition)
                            return;
                        SensorRecord record;
                        synchronized (lock) {
                            lamport[0] = Math.max(lamport[0], lm) + 1;
                            vector[0].merge(vm);
                            vector[0].tick(partition);
                            record = SensorRecord.now(++seq[0], sensor, temp, lamport[0], vector[0]);
                            if (record.getVector().get(partition) != record.getSeq())
                                throw new IllegalStateException("Relogio vetorial inconsistente");
                            file.append(record);
                            pending.add(record);
                        }
                        System.out.println("Registado: " + record.toLine());
                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + message);
                    }
                });
            } catch (Exception e) {
                System.out.println("Subscricao interrompida: " + e.getMessage());
            }
        });
        input.start();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> sendBatch(host, port, pub, partition, lock, pending, inc, hb, seq, file), 0,
                Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("Lider p" + partition + " a escuta no porto " + port);
            while (true)
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(Wire.T_TCP_MS);
                    serve(socket, file, lock, seq);
                } catch (IOException e) {
                    System.out.println("Recuperacao interrompida: " + e.getMessage());
                }
        } catch (IOException e) {
            System.out.println("Servidor: " + e.getMessage());
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static void sendBatch(String host, int port, IpfsPubSub pub, int k, Object lock, List<SensorRecord> pending,
            long[] inc, long[] hb, long[] seq, RecordFile file) {
        List<SensorRecord> records;
        long last, number;
        synchronized (lock) {
            records = new ArrayList<>(pending);
            pending.clear();
            last = seq[0];
            number = ++hb[0];
        }
        Batch batch = new Batch(host, port, inc[0], number, last, records);
        try {
            String encoded = batch.encode();
            pub.publish(Wire.topicData(k), encoded);
            try (FileWriter w = new FileWriter("hb-p" + k + ".txt", true)) {
                w.write(inc[0] + ";" + number + ";" + System.currentTimeMillis() + ";" + last + ";" + records.size()
                        + ";" + encoded.getBytes(StandardCharsets.UTF_8).length + System.lineSeparator());
            }
            System.out.println("HEARTBEAT: hb=" + number + " n=" + records.size() + " ultimoSeq=" + last);
        } catch (Exception e) {
            System.out.println("PUBLICACAO LOTE FALHOU: " + e.getMessage());
        }
    }

    private static void serve(Socket socket, RecordFile file, Object lock, long[] seq) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8),
                true);
        String[] f = Optional.ofNullable(in.readLine()).orElse("").trim().split("\\s+");
        if (f.length != 3 || !"PEDIDO".equals(f[0])) {
            out.println("ERRO pedido invalido");
            return;
        }
        try {
            long from = Long.parseLong(f[1]), to = Long.parseLong(f[2]);
            if (from > to) {
                out.println("ERRO intervalo invalido");
                return;
            }
            List<SensorRecord> rs;
            synchronized (lock) {
                to = Math.min(to, seq[0]);
                rs = file.range(from, to);
            }
            for (SensorRecord r : rs)
                out.println(r.toLine());
            out.println("FIM");
        } catch (NumberFormatException e) {
            out.println("ERRO numeros invalidos");
        }
    }
}
