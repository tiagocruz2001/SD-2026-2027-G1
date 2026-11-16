package raft04;

import java.util.ArrayList;
import java.util.List;

public final class Batch {
    public final long term;
    public final int leader;
    public final String address;
    public final int port;
    public final long hb;
    public final long lastSeq;
    public final long lastTerm;
    public final int n;
    public final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm,
            List<SensorRecord> records) {
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.n = records == null ? 0 : records.size();
        this.records = new ArrayList<>(records == null ? List.of() : records);
    }

    public String encode() {
        StringBuilder result = new StringBuilder("LOTE;")
                .append(term).append(';').append(leader).append(';')
                .append(address).append(';').append(port).append(';')
                .append(hb).append(';').append(lastSeq).append(';')
                .append(lastTerm).append(';').append(records.size());
        for (SensorRecord record : records) {
            result.append('\n').append(record.toLine());
        }
        return result.toString();
    }

    public static Batch decode(String message, int expectedRecords) {
        return decode(message, expectedRecords, 3);
    }

    public static Batch decode(String message, int expectedRecords, int partitionCount) {
        String[] lines = message.trim().split("\\R", -1);
        String[] header = lines[0].split(";", -1);
        if (header.length != 9 || !"LOTE".equals(header[0])) {
            throw new IllegalArgumentException("LOTE mal formado: " + message);
        }
        int count = Integer.parseInt(header[8]);
        if (count != expectedRecords || lines.length - 1 != count) {
            throw new IllegalArgumentException("numero de registos inconsistente");
        }
        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            records.add(SensorRecord.fromLine(lines[i], partitionCount));
        }
        return new Batch(Long.parseLong(header[1]), Integer.parseInt(header[2]), header[3],
                Integer.parseInt(header[4]), Long.parseLong(header[5]), Long.parseLong(header[6]),
                Long.parseLong(header[7]), records);
    }
}
