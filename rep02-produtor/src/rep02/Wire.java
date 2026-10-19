package rep02;

public final class Wire {
    private Wire() {
    }

    public static String topicIn(int k) {
        return "rep02-in-p" + k;
    }

    public static String in(String sensor, double temperature) {
        return "IN;" + sensor + ";" + temperature;
    }
}
