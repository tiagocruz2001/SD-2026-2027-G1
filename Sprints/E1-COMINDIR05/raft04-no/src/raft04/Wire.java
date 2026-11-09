package raft04;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class Wire {
    public static final int PORTO_BASE = 7000;
    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000L);
    public static final int FATOR_TIMEOUT = Integer.getInteger("raft04.fator", 3);
    public static final long VERIFICACAO_MS = 200L;
    public static final long ELEICAO_MIN_MS = Long.getLong("raft04.eleicaoMin", 3L * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS = Long.getLong("raft04.eleicaoMax", 6L * PERIODO_HB_MS);
    public static final int GRUPO = 3;

    private Wire() {
    }

    public static String topicIn(int k) {
        return "raft04-in-p" + k;
    }

    public static String topicData(int k) {
        return "raft04-p" + k;
    }

    public static String topicElection(int k) {
        return "raft04-eleicao-p" + k;
    }

    public static String in(String sensor, double temperature, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temperature + ";" + lamport + ";" + vector;
    }

    public static int nodePort(int k, int i) {
        return PORTO_BASE + 100 * k + i;
    }

    public static boolean isolated(int k, int i) {
        return Files.exists(Paths.get("isolar-p" + k + "-n" + i));
    }

    public static String type(String message) {
        int index = message.trim().indexOf(';');
        if (index <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + message);
        }
        return message.trim().substring(0, index);
    }

    public static String body(String message) {
        String value = message.trim();
        int index = value.indexOf(';');
        if (index < 0) {
            throw new IllegalArgumentException("Mensagem sem corpo: " + message);
        }
        return value.substring(index + 1);
    }
}
