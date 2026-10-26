package rep03;

import java.time.LocalDateTime;

public class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, String sensorId, double temperature, String timestamp, long lamport,
            VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector.copy();

    }

    public static SensorRecord now(long seq, String sensor, double temperature, long lamport, VectorClock vector) {
        return new SensorRecord(seq, sensor, temperature, LocalDateTime.now().withNano(0).toString(), lamport, vector);
    }

    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector.toString();
    }

    public static SensorRecord fromLine(String line) {
        String[] fields = line.trim().split(";", -1);
        if (fields.length != 6)
            throw new IllegalArgumentException("Registo mal formado: " + line);
        return new SensorRecord(Long.parseLong(fields[0]), fields[1], Double.parseDouble(fields[2]), fields[3],
                Long.parseLong(fields[4]), VectorClock.parse(fields[5], 3));
    }

    public long getSeq() {
        return seq;
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
