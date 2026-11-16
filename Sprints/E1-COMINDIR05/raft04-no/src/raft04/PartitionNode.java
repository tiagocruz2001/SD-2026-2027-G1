package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class PartitionNode {
    private static final int TCP_TIMEOUT_MS = 5000;

    private final int partition;
    private final int id;
    private final int partitionCount;
    private final String address;
    private final IpfsPubSub pub;
    private final RecordFile file;
    private final Path nodePath;
    private final Path heartbeatPath;
    private final RaftState raft;
    private final Partitioner partitioner;
    private final Consumer<String> log;
    private final List<SensorRecord> records = new ArrayList<>();
    private final List<SensorRecord> pending = new ArrayList<>();
    private final ScheduledExecutorService electionScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService leaderScheduler = Executors.newSingleThreadScheduledExecutor();

    private long seq;
    private long lamport;
    private long heartbeat;
    private VectorClock vector;
    private ScheduledFuture<?> leaderTask;

    private PartitionNode(int partition, int id, int partitionCount, String api, String address) throws IOException {
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("Particao invalida: " + partition);
        }
        if (id < 0 || id >= Wire.GRUPO) {
            throw new IllegalArgumentException("O identificador do no tem de estar entre 0 e " + (Wire.GRUPO - 1));
        }
        if (Wire.ELEICAO_MIN_MS <= 0 || Wire.ELEICAO_MAX_MS < Wire.ELEICAO_MIN_MS) {
            throw new IllegalArgumentException("Intervalo de eleicao invalido");
        }

        this.partition = partition;
        this.id = id;
        this.partitionCount = partitionCount;
        this.address = address;
        this.pub = new IpfsPubSub(api);
        this.partitioner = new Partitioner(partitionCount);
        this.file = new RecordFile(nodeFileName(partition, id), partitionCount);
        this.nodePath = Paths.get(nodeFileName(partition, id));
        this.heartbeatPath = Paths.get("hb-p" + partition + "-n" + id + ".txt");
        Path statePath = Paths.get("estado-p" + partition + "-n" + id + ".txt");
        Path logPath = Paths.get("eventos-p" + partition + "-n" + id + ".txt");
        this.log = message -> appendEvent(logPath, message);

        records.addAll(file.readAll());
        long expectedSeq = 1;
        for (SensorRecord record : records) {
            if (record.getSeq() != expectedSeq++) {
                throw new IOException("Sequencia local nao contigua em " + nodePath);
            }
        }
        SensorRecord last = records.isEmpty() ? null : records.get(records.size() - 1);
        seq = last == null ? 0 : last.getSeq();
        lamport = last == null ? 0 : last.getLamport();
        vector = last == null ? new VectorClock(partitionCount) : last.getVector();

        raft = new RaftState(partition, id, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS,
                statePath, log);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionNode <k> <i> [N] [api] [endereco]");
            return;
        }
        int partition = Integer.parseInt(args[0]);
        int id = Integer.parseInt(args[1]);
        int partitionCount = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        String api = args.length > 3 ? args[3]
                : System.getProperty("raft04.api", "http://127.0.0.1:5001/api/v0");
        String address = args.length > 4 ? args[4]
                : System.getProperty("raft04.address", "127.0.0.1");

        PartitionNode node = new PartitionNode(partition, id, partitionCount, api, address);
        node.start();
    }

    private static String nodeFileName(int partition, int id) {
        return "no-p" + partition + "-n" + id + ".txt";
    }

    private static void appendEvent(Path path, String message) {
        String line = System.currentTimeMillis() + ";" + message;
        System.out.println(line);
        try {
            Files.writeString(path, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("Nao foi possivel escrever " + path, e);
        }
    }

    private void start() throws IOException, InterruptedException {
        subscribe(Wire.topicIn(partition), this::onInput, "entrada");
        subscribe(Wire.topicData(partition), this::onBatchMessage, "dados");
        subscribe(Wire.topicElection(partition), this::onElectionMessage, "eleicao");

        Thread server = new Thread(this::serveRecoveryRequests, "tcp-p" + partition + "-n" + id);
        server.setDaemon(true);
        server.start();

        electionScheduler.scheduleAtFixedRate(this::checkElection, Wire.VERIFICACAO_MS,
                Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        log.accept("ARRANCAR;p" + partition + ";n" + id + ";termo=" + raft.term());
        new CountDownLatch(1).await();
    }

    private void subscribe(String topic, Consumer<String> handler, String label) {
        Thread thread = new Thread(() -> {
            try {
                pub.subscribe(topic, message -> {
                    try {
                        handler.accept(message);
                    } catch (RuntimeException e) {
                        log.accept("ERRO;subscricao=" + label + ";mensagem=" + e.getMessage());
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.accept("SUBSCRICAO_INTERROMPIDA;" + label);
            } catch (Exception e) {
                log.accept("SUBSCRICAO_FALHOU;" + label + ";" + e.getMessage());
            }
        }, "sub-" + label + "-p" + partition + "-n" + id);
        thread.setDaemon(true);
        thread.start();
    }

    private void checkElection() {
        long candidateTerm;
        try {
            candidateTerm = raft.tick();
        } catch (RuntimeException e) {
            log.accept("ERRO;tick;" + e.getMessage());
            return;
        }
        if (candidateTerm <= 0) {
            return;
        }
        VoteRequest request;
        synchronized (this) {
            request = new VoteRequest(candidateTerm, id, seq, lastRecordTerm());
        }
        publish(Wire.topicElection(partition), request.encode(), "PEDIDO_VOTO");
    }

    private void onElectionMessage(String message) {
        if (Wire.isolated(partition, id)) {
            return;
        }
        String type = Wire.type(message);
        if ("PEDIDO_VOTO".equals(type)) {
            onVoteRequest(VoteRequest.decode(message));
        } else if ("VOTO".equals(type)) {
            onVoteReply(VoteReply.decode(message));
        } else {
            throw new IllegalArgumentException("Mensagem de eleicao desconhecida: " + type);
        }
    }

    private void onVoteRequest(VoteRequest request) {
        validateNodeId(request.candidate);
        if (request.candidate == id) {
            return;
        }

        RaftState.Vote result;
        synchronized (this) {
            result = raft.onVoteRequest(request.term, request.candidate, request.lastSeq, request.lastTerm,
                    seq, lastRecordTerm());
            stopLeaderIfDemoted();
        }
        VoteReply reply = new VoteReply(result.term, id, request.candidate, result.granted);
        publish(Wire.topicElection(partition), reply.encode(), "VOTO");
    }

    private void onVoteReply(VoteReply reply) {
        validateNodeId(reply.voter);
        validateNodeId(reply.candidate);
        if (reply.voter == id || reply.candidate != id) {
            return;
        }

        boolean becameLeader;
        synchronized (this) {
            becameLeader = raft.onVote(reply.term, reply.voter, reply.candidate, reply.granted);
            stopLeaderIfDemoted();
            if (becameLeader) {
                startLeader();
            }
        }
    }

    private void onInput(String message) {
        if (!"IN".equals(Wire.type(message))) {
            throw new IllegalArgumentException("Mensagem de entrada desconhecida");
        }
        String[] fields = Wire.body(message).split(";", -1);
        if (fields.length != 4) {
            throw new IllegalArgumentException("IN mal formado: " + message);
        }
        String sensor = fields[0];
        double temperature = Double.parseDouble(fields[1]);
        long incomingLamport = Long.parseLong(fields[2]);
        VectorClock incomingVector = VectorClock.parse(fields[3], partitionCount);
        if (partitioner.of(sensor) != partition) {
            return;
        }

        synchronized (this) {
            long term = raft.termIfLeader();
            if (term < 0) {
                log.accept("IGNORADO;p" + partition + ";IN;" + sensor + ";termo=" + raft.term());
                return;
            }

            long nextSeq = seq + 1;
            long nextLamport = Math.max(lamport, incomingLamport) + 1;
            VectorClock nextVector = vector.copy();
            nextVector.merge(incomingVector);
            nextVector.tick(partition);
            if (nextVector.get(partition) != nextSeq) {
                throw new IllegalStateException("Relogio vetorial inconsistente: V[" + partition + "]="
                        + nextVector.get(partition) + ", seq=" + nextSeq);
            }
            SensorRecord record = SensorRecord.now(nextSeq, term, sensor, temperature, nextLamport, nextVector);
            try {
                file.append(record);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException("Nao foi possivel gravar leitura do lider", e);
            }
            records.add(record);
            pending.add(record);
            seq = nextSeq;
            lamport = nextLamport;
            vector = nextVector;
            log.accept("REGISTADO;p" + partition + ";n" + id + ";" + record.toLine());
        }
    }

    private void startLeader() {
        if (raft.termIfLeader() < 0) {
            return;
        }
        if (leaderTask != null && !leaderTask.isDone()) {
            leaderTask.cancel(false);
        }
        pending.clear();
        heartbeat = 0;
        SensorRecord last = records.isEmpty() ? null : records.get(records.size() - 1);
        seq = last == null ? 0 : last.getSeq();
        lamport = last == null ? 0 : last.getLamport();
        vector = last == null ? new VectorClock(partitionCount) : last.getVector();
        leaderTask = leaderScheduler.scheduleAtFixedRate(this::sendBatch, 0,
                Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
        log.accept("INICIO_LOTE;p" + partition + ";n" + id + ";termo=" + raft.term());
    }

    private void stopLeaderIfDemoted() {
        if (raft.termIfLeader() < 0 && leaderTask != null) {
            leaderTask.cancel(false);
            leaderTask = null;
        }
    }

    private void sendBatch() {
        if (Wire.isolated(partition, id)) {
            return;
        }
        final Batch batch;
        final String encoded;
        synchronized (this) {
            long term = raft.termIfLeader();
            if (term < 0) {
                stopLeaderIfDemoted();
                return;
            }
            List<SensorRecord> batchRecords = new ArrayList<>(pending);
            pending.clear();
            SensorRecord last = records.isEmpty() ? null : records.get(records.size() - 1);
            long lastSeq = last == null ? 0 : last.getSeq();
            long lastTerm = last == null ? 0 : last.getTerm();
            batch = new Batch(term, id, address, Wire.nodePort(partition, id),
                    ++heartbeat, lastSeq, lastTerm, batchRecords);
            encoded = batch.encode();
        }

        try {
            pub.publish(Wire.topicData(partition), encoded);
        } catch (Exception e) {
            synchronized (this) {
                if (raft.termIfLeader() == batch.term) {
                    Map<Long, SensorRecord> merged = new TreeMap<>();
                    for (SensorRecord record : batch.records) {
                        merged.put(record.getSeq(), record);
                    }
                    for (SensorRecord record : pending) {
                        merged.put(record.getSeq(), record);
                    }
                    pending.clear();
                    pending.addAll(merged.values());
                }
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.accept("PUBLICACAO_LOTE_FALHOU;p" + partition + ";termo=" + batch.term + ";" + e.getMessage());
            return;
        }

        try {
            String line = batch.term + ";" + batch.hb + ";" + System.currentTimeMillis() + ";"
                    + batch.lastSeq + ";" + batch.n + ";"
                    + encoded.getBytes(StandardCharsets.UTF_8).length + System.lineSeparator();
            Files.writeString(heartbeatPath, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.accept("HEARTBEAT_LOG_FALHOU;p" + partition + ";termo=" + batch.term + ";" + e.getMessage());
        }
        log.accept("LOTE;p" + partition + ";n" + id + ";termo=" + batch.term
                + ";hb=" + batch.hb + ";ultimoSeq=" + batch.lastSeq + ";n=" + batch.n);
    }

    private void onBatchMessage(String message) {
        if (Wire.isolated(partition, id)) {
            return;
        }
        if (!"LOTE".equals(Wire.type(message))) {
            throw new IllegalArgumentException("Mensagem de dados desconhecida");
        }
        String headerLine = message.split("\\R", 2)[0];
        String[] header = headerLine.split(";", -1);
        if (header.length != 9) {
            throw new IllegalArgumentException("Cabecalho LOTE mal formado");
        }
        int count = Integer.parseInt(header[8]);
        Batch batch = Batch.decode(message, count, partitionCount);
        validateNodeId(batch.leader);
        if (batch.leader == id) {
            return;
        }

        synchronized (this) {
            if (!raft.onBatch(batch.term, batch.leader)) {
                log.accept("IGNORADO;p" + partition + ";termo=" + batch.term + ";lider=n" + batch.leader);
                return;
            }
            stopLeaderIfDemoted();
        }
        log.accept("RECEBIDO;p" + partition + ";termo=" + batch.term + ";lider=n"
                + batch.leader + ";hb=" + batch.hb + ";ultimoSeq=" + batch.lastSeq + ";n=" + batch.n);

        boolean needsSync = applyBatchRecords(batch);
        synchronized (this) {
            SensorRecord last = records.isEmpty() ? null : records.get(records.size() - 1);
            long localSeq = last == null ? 0 : last.getSeq();
            long localTerm = last == null ? 0 : last.getTerm();
            needsSync |= localSeq != batch.lastSeq || localTerm != batch.lastTerm;
        }
        if (needsSync) {
            reconcile(batch);
        }
    }

    private boolean applyBatchRecords(Batch batch) {
        boolean needsSync = false;
        List<SensorRecord> incoming = new ArrayList<>(batch.records);
        incoming.sort(Comparator.comparingLong(SensorRecord::getSeq));
        synchronized (this) {
            for (SensorRecord record : incoming) {
                if (record.getSeq() <= seq) {
                    SensorRecord local = recordAt(record.getSeq());
                    if (local == null || local.getTerm() != record.getTerm()) {
                        needsSync = true;
                        log.accept("DIVERGENCIA;p" + partition + ";seq=" + record.getSeq()
                                + ";termoLocal=" + (local == null ? -1 : local.getTerm())
                                + ";termoLote=" + record.getTerm());
                    }
                } else if (record.getSeq() == seq + 1) {
                    appendFollowerRecord(record);
                } else {
                    needsSync = true;
                }
            }
        }
        return needsSync;
    }

    private void appendFollowerRecord(SensorRecord record) {
        try {
            file.append(record);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("Nao foi possivel gravar registo replicado", e);
        }
        records.add(record);
        seq = record.getSeq();
        lamport = record.getLamport();
        vector = record.getVector();
    }

    private void reconcile(Batch batch) {
        if (Wire.isolated(partition, id)) {
            return;
        }
        try {
            List<SensorRecord> local;
            synchronized (this) {
                local = new ArrayList<>(records);
            }

            if (local.isEmpty() && batch.lastSeq > 0) {
                List<SensorRecord> recovered = requestRecords(batch.address, batch.port, 1, batch.lastSeq);
                appendRecovered(recovered, 1, batch.lastSeq);
                log.accept("RECUPERACAO;p" + partition + ";de=1;ate=" + batch.lastSeq);
                return;
            }

            long start = local.isEmpty() ? 1 : lastTermBlockStart(local);
            List<Long> starts = termBlockStartsDescending(local);
            List<SensorRecord> remote = new ArrayList<>();
            boolean boundaryMatches = false;
            int startIndex = 0;
            while (true) {
                remote = requestRecords(batch.address, batch.port, start, batch.lastSeq);
                SensorRecord localAtStart = recordAt(local, start);
                SensorRecord remoteAtStart = recordAt(remote, start);
                boundaryMatches = localAtStart != null && remoteAtStart != null
                        && localAtStart.toLine().equals(remoteAtStart.toLine());
                if (boundaryMatches || start <= 1) {
                    break;
                }
                while (startIndex < starts.size() && starts.get(startIndex) >= start) {
                    startIndex++;
                }
                if (startIndex >= starts.size()) {
                    start = 1;
                } else {
                    start = starts.get(startIndex);
                }
            }

            long divergence = -1;
            if (!boundaryMatches) {
                divergence = 1;
            } else {
                Map<Long, SensorRecord> localBySeq = indexBySeq(local);
                for (SensorRecord leaderRecord : remote) {
                    SensorRecord localRecord = localBySeq.get(leaderRecord.getSeq());
                    if (localRecord != null && !localRecord.toLine().equals(leaderRecord.toLine())) {
                        divergence = leaderRecord.getSeq();
                        break;
                    }
                }
                long localLast = local.isEmpty() ? 0 : local.get(local.size() - 1).getSeq();
                if (divergence < 0 && localLast > batch.lastSeq) {
                    divergence = batch.lastSeq + 1;
                }
            }

            if (divergence >= 0) {
                truncateAndReplace(local, remote, divergence, batch.lastSeq);
                return;
            }

            long oldLast;
            synchronized (this) {
                oldLast = seq;
            }
            if (oldLast < batch.lastSeq) {
                List<SensorRecord> missing = requestRecords(batch.address, batch.port, oldLast + 1, batch.lastSeq);
                appendRecovered(missing, oldLast + 1, batch.lastSeq);
                log.accept("RECUPERACAO;p" + partition + ";de=" + (oldLast + 1) + ";ate=" + batch.lastSeq);
            }
        } catch (Exception e) {
            log.accept("RECUPERACAO_FALHOU;p" + partition + ";lider=n" + batch.leader + ";" + e.getMessage());
        }
    }

    private long lastTermBlockStart(List<SensorRecord> local) {
        if (local.isEmpty()) {
            return 1;
        }
        long term = local.get(local.size() - 1).getTerm();
        int index = local.size() - 1;
        while (index > 0 && local.get(index - 1).getTerm() == term) {
            index--;
        }
        return local.get(index).getSeq();
    }

    private List<Long> termBlockStartsDescending(List<SensorRecord> local) {
        List<Long> starts = new ArrayList<>();
        long previous = Long.MIN_VALUE;
        for (int index = local.size() - 1; index >= 0; index--) {
            long term = local.get(index).getTerm();
            if (term != previous) {
                starts.add(local.get(index).getSeq());
                previous = term;
            }
        }
        return starts;
    }

    private List<SensorRecord> requestRecords(String host, int port, long from, long to) throws IOException {
        if (from > to) {
            return new ArrayList<>();
        }
        if (Wire.isolated(partition, id)) {
            throw new IOException("No isolado nao pode recuperar registos");
        }
        List<SensorRecord> result = new ArrayList<>();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), TCP_TIMEOUT_MS);
            socket.setSoTimeout(TCP_TIMEOUT_MS);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            writer.println("PEDIDO " + from + " " + to);
            String line;
            long expected = from;
            while ((line = reader.readLine()) != null && !"FIM".equals(line)) {
                if (line.startsWith("ERRO")) {
                    throw new IOException(line);
                }
                SensorRecord record = SensorRecord.fromLine(line, partitionCount);
                if (record.getSeq() != expected) {
                    throw new IOException("Sequencia TCP inesperada: esperava " + expected
                            + ", recebeu " + record.getSeq());
                }
                result.add(record);
                expected++;
            }
            if (expected <= to) {
                throw new IOException("Recuperacao TCP incompleta: faltam registos desde " + expected);
            }
        }
        return result;
    }

    private void appendRecovered(List<SensorRecord> recovered, long from, long to) throws IOException {
        synchronized (this) {
            long expected = seq + 1;
            for (SensorRecord record : recovered) {
                if (record.getSeq() != expected) {
                    throw new IOException("Lacuna na recuperacao: esperava " + expected);
                }
                file.append(record);
                records.add(record);
                seq = record.getSeq();
                lamport = record.getLamport();
                vector = record.getVector();
                expected++;
            }
            if (seq < to || (recovered.isEmpty() && from <= to)) {
                throw new IOException("Registos recuperados insuficientes para o intervalo " + from + ".." + to);
            }
        }
    }

    private void truncateAndReplace(List<SensorRecord> local, List<SensorRecord> remote,
            long divergence, long leaderLastSeq) throws IOException {
        List<SensorRecord> replacement = new ArrayList<>();
        for (SensorRecord record : local) {
            if (record.getSeq() < divergence) {
                replacement.add(record);
            }
        }
        for (SensorRecord record : remote) {
            if (record.getSeq() >= divergence && record.getSeq() <= leaderLastSeq) {
                replacement.add(record);
            }
        }
        replacement.sort(Comparator.comparingLong(SensorRecord::getSeq));

        List<SensorRecord> discarded = new ArrayList<>();
        for (SensorRecord record : local) {
            if (record.getSeq() >= divergence) {
                discarded.add(record);
            }
        }
        file.replaceAll(replacement);
        synchronized (this) {
            records.clear();
            records.addAll(replacement);
            pending.clear();
            SensorRecord last = records.isEmpty() ? null : records.get(records.size() - 1);
            seq = last == null ? 0 : last.getSeq();
            lamport = last == null ? 0 : last.getLamport();
            vector = last == null ? new VectorClock(partitionCount) : last.getVector();
        }
        LinkedHashSet<Long> terms = new LinkedHashSet<>();
        for (SensorRecord record : discarded) {
            terms.add(record.getTerm());
        }
        log.accept("TRUNCADO;p" + partition + ";de=" + divergence + ";descartados="
                + discarded.size() + ";termos=" + terms);
    }

    private void serveRecoveryRequests() {
        int port = Wire.nodePort(partition, id);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log.accept("TCP_ESCUTA;p" + partition + ";n" + id + ";porto=" + port);
            while (!Thread.currentThread().isInterrupted()) {
                try (Socket socket = serverSocket.accept()) {
                    if (Wire.isolated(partition, id)) {
                        continue;
                    }
                    socket.setSoTimeout(TCP_TIMEOUT_MS);
                    serve(socket);
                } catch (IOException e) {
                    log.accept("TCP_PEDIDO_FALHOU;p" + partition + ";n" + id + ";" + e.getMessage());
                }
            }
        } catch (IOException e) {
            log.accept("TCP_SERVIDOR_FALHOU;p" + partition + ";n" + id + ";" + e.getMessage());
        }
    }

    private void serve(Socket socket) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
        String line = reader.readLine();
        if (line == null || Wire.isolated(partition, id)) {
            return;
        }
        String[] fields = line.trim().split("\\s+");
        if (fields.length != 3 || !"PEDIDO".equals(fields[0])) {
            writer.println("ERRO pedido invalido");
            return;
        }
        long from;
        long to;
        try {
            from = Long.parseLong(fields[1]);
            to = Long.parseLong(fields[2]);
        } catch (NumberFormatException e) {
            writer.println("ERRO intervalo invalido");
            return;
        }
        if (from < 1 || from > to) {
            writer.println("ERRO intervalo invalido");
            return;
        }

        List<SensorRecord> requested = file.range(from, to);
        long expected = from;
        for (SensorRecord record : requested) {
            if (Wire.isolated(partition, id)) {
                return;
            }
            if (record.getSeq() != expected++) {
                writer.println("ERRO sequencia local incompleta");
                return;
            }
            writer.println(record.toLine());
        }
        writer.println("FIM");
    }

    private void publish(String topic, String message, String type) {
        if (Wire.isolated(partition, id)) {
            log.accept("PUBLICACAO_SUPRIMIDA;p" + partition + ";n" + id + ";tipo=" + type + ";isolado");
            return;
        }
        try {
            pub.publish(topic, message);
            log.accept("PUBLICADO;p" + partition + ";n" + id + ";tipo=" + type);
        } catch (Exception e) {
            log.accept("PUBLICACAO_FALHOU;p" + partition + ";n" + id + ";tipo=" + type + ";" + e.getMessage());
        }
    }

    private void validateNodeId(int nodeId) {
        if (nodeId < 0 || nodeId >= Wire.GRUPO) {
            throw new IllegalArgumentException("Identificador de no invalido: " + nodeId);
        }
    }

    private synchronized long lastRecordTerm() {
        return records.isEmpty() ? 0 : records.get(records.size() - 1).getTerm();
    }

    private synchronized SensorRecord recordAt(long sequence) {
        if (sequence < 1 || sequence > records.size()) {
            return null;
        }
        SensorRecord record = records.get((int) sequence - 1);
        return record.getSeq() == sequence ? record : null;
    }

    private static SensorRecord recordAt(List<SensorRecord> entries, long sequence) {
        if (sequence < 1 || sequence > entries.size()) {
            return null;
        }
        SensorRecord record = entries.get((int) sequence - 1);
        return record.getSeq() == sequence ? record : null;
    }

    private static Map<Long, SensorRecord> indexBySeq(List<SensorRecord> entries) {
        Map<Long, SensorRecord> result = new TreeMap<>();
        for (SensorRecord record : entries) {
            result.put(record.getSeq(), record);
        }
        return result;
    }
}