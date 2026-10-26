package rep03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class PartitionReplica {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <particoes> [portoApi]");
            return;
        }
        String id = args[0];
        String api = args.length > 2 ? "http://127.0.0.1:" + args[2] + "/api/v0" : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub pubSub = new IpfsPubSub(api);
        for (String value : args[1].split(",")) {
            int partition = Integer.parseInt(value.trim());
            State state;
            try {
                state = new State(id, partition);
            } catch (IOException e) {
                System.out.println("IO: " + e.getMessage());
                continue;
            }
            int currentPartition = partition;
            Thread thread = new Thread(() -> {
                try {
                    pubSub.subscribe(Wire.topicData(currentPartition), state::apply);
                } catch (Exception e) {
                    System.out.println("Subscricao p" + currentPartition + " interrompida: " + e.getMessage());
                }
            }, "replica-p" + currentPartition);
            thread.start();
        }
    }

    private static final class State {
        private final RecordFile file;
        private final Map<Long, SensorRecord> pending = new TreeMap<>();
        private final Random random = new Random();
        private long last;
        private String host;
        private int port;
        private boolean discarded;

        State(String id, int partition) throws IOException {
            file = new RecordFile("replica-" + id + "-p" + partition + ".txt");
            last = file.lastSeq();
        }

        private synchronized void apply(String message) {
            try {
                String[] parts = Wire.regParts(message);
                host = parts[0];
                port = Integer.parseInt(parts[1]);
                SensorRecord record = SensorRecord.fromLine(parts[2]);
                String discard = System.getProperty("descartar");
                if (discard != null && !discarded && record.getSeq() == Long.parseLong(discard)) {
                    discarded = true;
                    System.out.println("DESCARTADO (simulacao): " + record.getSeq());
                    return;
                }
                double loss = Double.parseDouble(System.getProperty("perda", "0"));
                if (loss > 0 && random.nextDouble() < loss) {
                    System.out.println("DESCARTADO (simulacao): " + record.getSeq());
                    return;
                }
                if (record.getSeq() <= last || pending.containsKey(record.getSeq())) {
                    System.out.println("DUPLICADO: " + record.getSeq());
                    return;
                }
                if (record.getSeq() > last + 1) {
                    pending.put(record.getSeq(), record);
                    System.out.println("EM ESPERA: " + record.getSeq() + " (falta o registo " + (last + 1) + ")");
                    if (pending.size() > Wire.LIMITE_RETIDOS)
                        recover();
                    return;
                }
                append(record);
                drain();
            } catch (Exception e) {
                System.out.println("REJEITADO: " + message);
            }
        }

        private void append(SensorRecord record) throws IOException {
            file.append(record);
            last = record.getSeq();
            System.out.println("Aplicado: " + record.toLine());
        }

        private void drain() throws IOException {
            while (pending.containsKey(last + 1))
                append(pending.remove(last + 1));
        }

        private void recover() {
            if (host == null || pending.isEmpty())
                return;
            long from = last + 1;
            long to = pending.keySet().iterator().next() - 1;
            if (from > to)
                return;
            System.out.println("RECUPERACAO: " + from + ".." + to + " pedida a " + host + ":" + port);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), Wire.T_TCP_MS);
                socket.setSoTimeout(Wire.T_TCP_MS);
                PrintWriter out = new PrintWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                out.println("PEDIDO " + from + " " + to);
                String line;
                boolean finished = false;
                while ((line = in.readLine()) != null) {
                    if (line.equals("FIM")) {
                        finished = true;
                        break;
                    }
                    if (line.startsWith("ERRO "))
                        throw new IOException(line);
                    SensorRecord record = SensorRecord.fromLine(line);
                    if (record.getSeq() == last + 1) {
                        append(record);
                        drain();
                    } else if (record.getSeq() > last + 1)
                        pending.put(record.getSeq(), record);
                    System.out.println("RECUPERADO: " + record.getSeq());
                }
                if (!finished)
                    throw new IOException("resposta sem FIM");
                drain();
            } catch (Exception e) {
                System.out.println("RECUPERACAO FALHOU: " + e.getMessage());
            }
        }
    }
}
