package rep02;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10; public static final int T_TCP_MS = 5000; public static final int PORTO_BASE = 7000;
    private Wire() { }
    public static String topicData(int k) { return "rep02-p" + k; }
    public static String[] regParts(String message) { String[] f = message.trim().split(";", 4); if (f.length != 4 || !f[0].equals("REG")) throw new IllegalArgumentException("REG mal formado: " + message); return new String[] { f[1], f[2], f[3] }; }
}
