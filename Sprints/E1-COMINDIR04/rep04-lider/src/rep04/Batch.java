package rep04;

import java.util.ArrayList;
import java.util.List;

public final class Batch {
    public final String address;
    public final int port;
    public final long inc;
    public final long hb;
    public final long lastSeq;
    public final List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb, long lastSeq,
            List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = new ArrayList<>(records);
    }

    public String encode() {
        StringBuilder result = new StringBuilder("LOTE;")
                .append(address).append(';').append(port).append(';')
                .append(inc).append(';').append(hb).append(';')
                .append(lastSeq).append(';').append(records.size());
        for (SensorRecord record : records)
            result.append('\n').append(record.toLine());
        return result.toString();
    }

    public static Batch decode(String message, int expectedRecords) {
        String[] lines = message.trim().split("\\R", -1);
        String[] header = lines[0].split(";", -1);
        if (header.length != 7 || !"LOTE".equals(header[0]))
            throw new IllegalArgumentException("LOTE mal formado");
        int count = Integer.parseInt(header[6]);
        if (count != expectedRecords || lines.length - 1 != count)
            throw new IllegalArgumentException("numero de registos inconsistente");
        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++)
            records.add(SensorRecord.fromLine(lines[i]));
        return new Batch(header[1], Integer.parseInt(header[2]), Long.parseLong(header[3]),
                Long.parseLong(header[4]), Long.parseLong(header[5]), records);
    }
}
