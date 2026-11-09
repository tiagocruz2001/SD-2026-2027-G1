package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class PartitionNode {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionNode <k> <i> [N]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        Path statePath = Paths.get("estado-p" + k + "-n" + i + ".txt");
        Path logPath = Paths.get("eventos-p" + k + "-n" + i + ".txt");
        Path nodePath = Paths.get("no-p" + k + "-n" + i + ".txt");

        long lastSeq = 0;
        long lastTerm = 0;
        if (Files.exists(nodePath)) {
            for (String line : Files.readAllLines(nodePath, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    SensorRecord record = SensorRecord.fromLine(line);
                    lastSeq = record.getSeq();
                    lastTerm = record.getTerm();
                }
            }
        }

        Consumer log = msg -> {
            String line = System.currentTimeMillis() + ";" + msg;
            System.out.println(line);
            try {
                Files.writeString(logPath, line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        };

        long initialTerm = 0;
        if (Files.exists(statePath)) {
            String[] state = Files.readString(statePath, StandardCharsets.UTF_8).trim().split(";");
            if (state.length >= 2) {
                initialTerm = Long.parseLong(state[0]);
            }
        }
        RaftState raft = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS, statePath, log::accept);
        if (raft.term() < initialTerm) {
            raft = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS, statePath, log::accept);
        }
        final RaftState raftFinal = raft;

        final long[] lastSeqRef = { lastSeq };
        final long[] lastTermRef = { lastTerm };
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            long term = raftFinal.tick();
            if (term > 0) {
                try {
                    VoteRequest req = new VoteRequest(term, i, lastSeqRef[0], lastTermRef[0]);
                    new IpfsPubSub("http://127.0.0.1:5001/api/v0").publish(Wire.topicElection(k), req.encode());
                } catch (Exception e) {
                    log.accept("ERRO;publicar voto:" + e.getMessage());
                }
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        Thread server = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(Wire.nodePort(k, i))) {
                while (!Thread.currentThread().isInterrupted()) {
                    try (Socket socket = serverSocket.accept()) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                        String line = reader.readLine();
                        if (line == null) {
                            continue;
                        }
                        String[] fields = line.split(" ");
                        if (fields.length >= 3 && "PEDIDO".equals(fields[0])) {
                            long from = Long.parseLong(fields[1]);
                            long to = Long.parseLong(fields[2]);
                            List<SensorRecord> records = new ArrayList<>();
                            if (Files.exists(nodePath)) {
                                for (String recordLine : Files.readAllLines(nodePath, StandardCharsets.UTF_8)) {
                                    if (!recordLine.isBlank()) {
                                        SensorRecord record = SensorRecord.fromLine(recordLine);
                                        if (record.getSeq() >= from && record.getSeq() <= to) {
                                            records.add(record);
                                        }
                                    }
                                }
                            }
                            for (SensorRecord record : records) {
                                writer.println(record.toLine());
                            }
                            writer.println("FIM");
                        }
                    }
                }
            } catch (IOException e) {
                log.accept("SERVIDOR;erro=" + e.getMessage());
            }
        });
        server.start();
        log.accept("ARRANCAR;p" + k + ";n" + i + ";termo=" + raft.term());
        while (true) {
            Thread.sleep(5000L);
        }
    }
}
