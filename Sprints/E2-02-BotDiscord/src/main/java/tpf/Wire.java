package tpf;

public final class Wire {
    public static final int CHUNK_MAX_BYTES =
            Integer.getInteger("tpf.chunkMaxBytes",
                    Integer.getInteger("tpf.chunk.max.bytes", 65536));
    public static final int PAD_BYTES =
            Integer.getInteger("tpf.padBytes",
                    Integer.getInteger("tpf.pad.bytes", 4096));
    public static final long SELAGEM_MS =
            Long.getLong("tpf.selagemMs",
                    Long.getLong("tpf.selagem.ms", 5000L));

    private Wire() {
    }
}
