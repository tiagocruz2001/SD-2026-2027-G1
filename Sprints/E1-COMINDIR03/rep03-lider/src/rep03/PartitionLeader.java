package rep03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereco] [api]");
            return;
        }

        int partition = Integer.parseInt(args[0]);
        int count = Integer.parseInt(args[1]);
        String host = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";
        if (partition < 0 || partition >= count) {
            System.out.println("Particao invalida");
            return;
        }

        RecordFile file = new RecordFile("lider-p" + partition + ".txt");
        Partitioner partitioner = new Partitioner(count);
        IpfsPubSub pubSub = new IpfsPubSub(api);
        int port = Wire.PORTO_BASE + partition;
        Object lock = new Object();
        long[] sequence = new long[1];
        final long[] lamport = new long[1];
        final VectorClock[] vector = { new VectorClock(count) };

        try {
            SensorRecord last = file.last();
            if (last != null) {
                sequence[0] = last.getSeq();
                lamport[0] = last.getLamport();
                vector[0] = last.getVector();
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
            return;
        }

        Thread input = new Thread(() -> {
            try {
                pubSub.subscribe(Wire.topicIn(partition), message -> {
                    try {
                        if (!Wire.type(message).equals("IN")) {
                            throw new IllegalArgumentException("tipo inesperado");
                        }
                        String[] fields = Wire.body(message).split(";", -1);
                        if (fields.length != 4) {
                            throw new IllegalArgumentException("IN mal formado");
                        }
                        String sensor = fields[0];
                        double temperature = Double.parseDouble(fields[1]);
                        long lamportMsg = Long.parseLong(fields[2]);
                        VectorClock vectorMsg = VectorClock.parse(fields[3], count);
                        int target = partitioner.of(sensor);
                        if (target != partition) {
                            System.out.println("PARTICAO ERRADA: " + sensor
                                    + " pertence a p" + target);
                            return;
                        }

                        SensorRecord record;
                        synchronized (lock) {
                            lamport[0] = Math.max(lamport[0], lamportMsg) + 1;
                            vector[0].merge(vectorMsg);
                            vector[0].tick(partition);
                            ++sequence[0];
                            if (vector[0].get(partition) != sequence[0]) {
                                throw new IllegalStateException("Relogio vetorial inconsistente");
                            }
                            record = SensorRecord.now(sequence[0], sensor, temperature, lamport[0], vector[0]);
                            file.append(record);
                        }
                        pubSub.publish(Wire.topicData(partition), Wire.reg(host, port, record));
                        System.out.println("Registado e publicado: " + record.toLine());
                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + message);
                    }
                });
            } catch (Exception e) {
                System.out.println("Subscricao interrompida: " + e.getMessage());
            }
        });
        input.start();

        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("Lider p" + partition + " a escuta no porto " + port);
            while (true) {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(Wire.T_TCP_MS);
                    serve(socket, file, lock, sequence);
                } catch (IOException e) {
                    System.out.println("Recuperacao interrompida: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Servidor: " + e.getMessage());
        }
    }

    private static void serve(Socket socket, RecordFile file, Object lock,
            long[] sequence) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(
                socket.getInputStream(), StandardCharsets.UTF_8));
        PrintWriter out = new PrintWriter(new OutputStreamWriter(
                socket.getOutputStream(), StandardCharsets.UTF_8), true);
        String request = in.readLine();
        String[] fields = request == null ? new String[0] : request.trim().split("\\s+");
        if (fields.length != 3 || !fields[0].equals("PEDIDO")) {
            out.println("ERRO pedido invalido");
            return;
        }

        try {
            long from = Long.parseLong(fields[1]);
            long to = Long.parseLong(fields[2]);
            if (from > to) {
                out.println("ERRO intervalo invalido");
                return;
            }
            List<SensorRecord> records;
            synchronized (lock) {
                to = Math.min(to, sequence[0]);
                records = file.range(from, to);
            }
            for (SensorRecord record : records) {
                out.println(record.toLine());
            }
            out.println("FIM");
            System.out.println("RECUPERACAO: " + from + ".." + to
                    + " (" + records.size() + " registos)");
        } catch (NumberFormatException e) {
            out.println("ERRO numeros invalidos");
        }
    }
}
