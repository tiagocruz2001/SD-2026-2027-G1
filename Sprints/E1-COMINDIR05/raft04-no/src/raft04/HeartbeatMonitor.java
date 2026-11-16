package raft04;

import java.util.function.Consumer;

public final class HeartbeatMonitor {
    public enum State {
        DESCONHECIDO, ATIVO, SUSPEITO
    }

    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;

    private State state = State.DESCONHECIDO;
    private long lastNanos = System.nanoTime();
    private long lastTerm = -1;
    private long lastHb = -1;

    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        this.partition = partition;
        this.timeoutNanos = periodMs * factor * 1_000_000L;
        this.log = log;
    }

    public synchronized void onHeartbeat(long term, int leader, long hb) {
        if (term < lastTerm) {
            return;
        }
        if (term > lastTerm) {
            if (lastTerm >= 0) {
                log.accept("NOVO_LIDER;p" + partition + ";termo=" + term + ";lider=n" + leader);
            }
            lastTerm = term;
            lastHb = hb;
            state = State.ATIVO;
            lastNanos = System.nanoTime();
            return;
        }
        if (hb > lastHb + 1 && lastHb >= 0) {
            log.accept("HB_PERDIDO;p" + partition + ";de=" + (lastHb + 1) + ";ate=" + (hb - 1));
        }
        lastHb = hb;
        state = State.ATIVO;
        lastNanos = System.nanoTime();
    }

    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            state = State.SUSPEITO;
            log.accept("SUSPEITA;p" + partition + ";silencio_ms=" + elapsed / 1_000_000L);
        }
    }

    public synchronized State state() {
        return state;
    }
}
