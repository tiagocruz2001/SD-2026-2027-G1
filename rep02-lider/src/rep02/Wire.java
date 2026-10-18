package rep02;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    private Wire() { }
    public static String topicIn(int k) { return "rep02-in-p" + k; }
    public static String topicData(int k) { return "rep02-p" + k; }
    public static String in(String sensor, double temperature) { return "IN;" + sensor + ";" + temperature; }
    public static String reg(String host, int port, SensorRecord record) { return "REG;" + host + ";" + port + ";" + record.toLine(); }
    public static String type(String message) { int i = message.trim().indexOf(';'); if (i <= 0) throw new IllegalArgumentException("Mensagem sem tipo: " + message); return message.trim().substring(0, i); }
    public static String body(String message) { String value = message.trim(); int i = value.indexOf(';'); if (i < 0) throw new IllegalArgumentException("Mensagem sem corpo: " + message); return value.substring(i + 1); }
    public static String[] regParts(String message) { String[] fields = message.trim().split(";", 4); if (fields.length != 4 || !fields[0].equals("REG")) throw new IllegalArgumentException("REG mal formado: " + message); return new String[] { fields[1], fields[2], fields[3] }; }
}
