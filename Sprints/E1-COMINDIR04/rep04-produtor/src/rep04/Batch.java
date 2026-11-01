package rep04;

import java.util.ArrayList;
import java.util.List;

public final class Batch {
    public final String address;
    public final int port;
    public final long inc, hb, lastSeq;
    public final List<SensorRecord> records;

    public Batch(String a, int p, long i, long h, long s, List<SensorRecord> r) {
        address = a;
        port = p;
        inc = i;
        hb = h;
        lastSeq = s;
        records = new ArrayList<>(r);
    }

    public String encode() {
        StringBuilder s = new StringBuilder("LOTE;").append(address).append(';').append(port).append(';').append(inc)
                .append(';').append(hb).append(';').append(lastSeq).append(';').append(records.size());
        for (SensorRecord r : records)
            s.append('\n').append(r.toLine());
        return s.toString();
    }

    public static Batch decode(String m, int expected) {
        String[] l = m.trim().split("\\R", -1), h = l[0].split(";", -1);
        if (h.length != 7 || !"LOTE".equals(h[0]))
            throw new IllegalArgumentException("LOTE mal formado");
        int n = Integer.parseInt(h[6]);
        if (n != expected || l.length - 1 != n)
            throw new IllegalArgumentException("numero de registos inconsistente");
        List<SensorRecord> r = new ArrayList<>();
        for (int i = 1; i < l.length; i++)
            r.add(SensorRecord.fromLine(l[i]));
        return new Batch(h[1], Integer.parseInt(h[2]), Long.parseLong(h[3]), Long.parseLong(h[4]), Long.parseLong(h[5]),
                r);
    }
}
