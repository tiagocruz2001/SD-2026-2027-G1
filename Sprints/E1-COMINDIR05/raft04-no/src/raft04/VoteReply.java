package raft04;

public final class VoteReply {
    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + granted;
    }

    public static VoteReply decode(String message) {
        String[] fields = message.trim().split(";", -1);
        if (fields.length != 5 || !"VOTO".equals(fields[0])) {
            throw new IllegalArgumentException("VOTO mal formado: " + message);
        }
        return new VoteReply(Long.parseLong(fields[1]), Integer.parseInt(fields[2]),
                Integer.parseInt(fields[3]), Boolean.parseBoolean(fields[4]));
    }
}
