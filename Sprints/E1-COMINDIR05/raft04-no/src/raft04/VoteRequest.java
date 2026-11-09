package raft04;

public final class VoteRequest {
    public final long term;
    public final int candidate;
    public final long lastSeq;
    public final long lastTerm;

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static VoteRequest decode(String message) {
        String[] fields = message.trim().split(";", -1);
        if (fields.length != 5 || !"PEDIDO_VOTO".equals(fields[0])) {
            throw new IllegalArgumentException("PEDIDO_VOTO mal formado: " + message);
        }
        return new VoteRequest(Long.parseLong(fields[1]), Integer.parseInt(fields[2]),
                Long.parseLong(fields[3]), Long.parseLong(fields[4]));
    }
}
