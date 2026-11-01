package rep04;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    public static final double LIMIAR_ALARME = 28.0;
    public static final long PERIODO_HB_MS = Long.getLong("rep04.periodo", 5000L);
    public static final int FATOR_TIMEOUT = Integer.getInteger("rep04.fator", 3);
    public static final long VERIFICACAO_MS = 1000L;
    public static String type(String message) { int i=message.indexOf(';'); return i<0?message:message.substring(0,i); }
    public static String body(String message) { int i=message.indexOf(';'); if(i<0) throw new IllegalArgumentException("sem corpo"); return message.substring(i+1); }
    
    private Wire() {
    }

    public static String topicData(int k) {
        return "rep04-p" + k;
    }

    public static String[] regParts(String message) {
        String[] f = message.trim().split(";", 4);
        if (f.length != 4 || !f[0].equals("REG"))
            throw new IllegalArgumentException("REG mal formado: " + message);
        return new String[] { f[1], f[2], f[3] };
    }
}
