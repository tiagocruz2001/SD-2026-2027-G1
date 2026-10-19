package rep02;

public final class Partitioner {
    private final int count;

    public Partitioner(int count) {
        if (count < 1)
            throw new IllegalArgumentException("Numero de particoes invalido: " + count);
        this.count = count;
    }

    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), count);
    }

    public int count() {
        return count;
    }
}
