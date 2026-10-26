package rep03;

public final class Wire {
    private Wire() {
    }

    public static String topicIn(int k) {
        return "rep03-in-p" + k;
    }

    public static String in(String sensor, double temperature, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temperature + ";" + lamport + ";" + vector.toString();
    }
}
