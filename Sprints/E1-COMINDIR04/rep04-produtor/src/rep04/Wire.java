package rep04;

public final class Wire {
    public static final long PERIODO_HB_MS = Long.getLong("rep04.periodo", 5000L);
    public static final int FATOR_TIMEOUT = Integer.getInteger("rep04.fator", 3);
    public static final long VERIFICACAO_MS = 1000L;
    public static String topicData(int k) { return "rep04-p" + k; }
    private Wire() {
    }

    public static String topicIn(int k) {
        return "rep04-in-p" + k;
    }

    public static String in(String sensor, double temperature, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temperature + ";" + lamport + ";" + vector.toString();
    }

    public static String body(String message) {
        int separator = message.indexOf(';');
        if (separator < 0) throw new IllegalArgumentException("Mensagem sem corpo");
        return message.substring(separator + 1);
    }
}
