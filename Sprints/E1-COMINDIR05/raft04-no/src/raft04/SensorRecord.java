package raft04;

import java.time.LocalDateTime;

public class SensorRecord {
    private final long seq;
    private final long term;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, long term, String sensorId, double temperature, String timestamp,
            long lamport, VectorClock vector) {
        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector.copy();
    }

    public static SensorRecord now(long seq, long term, String sensor, double temperature, long lamport,
            VectorClock vector) {
        return new SensorRecord(seq, term, sensor, temperature, LocalDateTime.now().withNano(0).toString(),
                lamport, vector);
    }

    public String toLine() {
        return seq + ";" + term + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";"
                + vector.toString();
    }

    public static SensorRecord fromLine(String line) {
        return fromLine(line, 3);
    }

    public static SensorRecord fromLine(String line, int partitionCount) {
        String[] fields = line.trim().split(";", -1);
        if (fields.length != 7) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(Long.parseLong(fields[0]), Long.parseLong(fields[1]), fields[2],
                Double.parseDouble(fields[3]), fields[4], Long.parseLong(fields[5]),
                VectorClock.parse(fields[6], partitionCount));
    }

    public long getSeq() {
        return seq;
    }

    public long getTerm() {
        return term;
    }

    public String getSensorId() {
        return sensorId;
    }

    public double getTemperature() {
        return temperature;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public long getLamport() {
        return lamport;
    }

    public VectorClock getVector() {
        return vector.copy();
    }
}
