package tpf;

import java.time.Instant;
import java.util.Objects;

public final class Registo {
    private final long lsn;
    private final Instant instante;
    private final String autor;
    private final String canal;
    private final String texto;

    public Registo(long lsn, Instant instante, String autor, String canal, String texto) {
        this.lsn = lsn;
        this.instante = Objects.requireNonNull(instante);
        this.autor = Objects.requireNonNull(autor);
        this.canal = Objects.requireNonNull(canal);
        this.texto = Objects.requireNonNull(texto);
    }

    public long getLsn() {
        return lsn;
    }

    public Instant getInstante() {
        return instante;
    }

    public String getAutor() {
        return autor;
    }

    public String getCanal() {
        return canal;
    }

    public String getTexto() {
        return texto;
    }
}
