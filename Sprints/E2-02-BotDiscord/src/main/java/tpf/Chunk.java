package tpf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Chunk {
    private final List<Registo> registos = new ArrayList<>();
    private long criadoEmMs;
    private boolean timerScheduled;

    public synchronized boolean isEmpty() {
        return registos.isEmpty();
    }

    public synchronized void add(Registo registo) {
        if (registos.isEmpty()) {
            criadoEmMs = System.currentTimeMillis();
        }
        registos.add(registo);
    }

    public synchronized long getCriadoEmMs() {
        return criadoEmMs;
    }

    public synchronized boolean consumeTimerScheduled() {
        if (timerScheduled) return false;
        timerScheduled = true;
        return true;
    }

    public synchronized List<Registo> getRegistos() {
        return Collections.unmodifiableList(new ArrayList<>(registos));
    }

    public synchronized long getMinLsn() {
        return registos.isEmpty() ? 0 : registos.get(0).getLsn();
    }

    public synchronized long getMaxLsn() {
        return registos.isEmpty() ? 0 : registos.get(registos.size() - 1).getLsn();
    }

    public synchronized int getSerializedSize() {
        try {
            return serializeRegistos(registos).length;
        } catch (IOException e) {
            throw new IllegalStateException("Falha a calcular o tamanho do chunk", e);
        }
    }

    public synchronized byte[] serialize() {
        try {
            byte[] dados = serializeRegistos(registos);
            int tamanho = dados.length;
            int comPadding = ((tamanho + Wire.PAD_BYTES - 1) / Wire.PAD_BYTES) * Wire.PAD_BYTES;
            if (comPadding == 0) {
                comPadding = Wire.PAD_BYTES;
            }
            byte[] padded = new byte[comPadding];
            System.arraycopy(dados, 0, padded, 0, dados.length);
            return padded;
        } catch (IOException e) {
            throw new IllegalStateException("Falha a serializar o chunk", e);
        }
    }

    public static List<Registo> deserialize(byte[] padded) {
        try (DataInputStream in =
                     new DataInputStream(new ByteArrayInputStream(padded))) {
            int count = in.readInt();
            if (count < 0 || count > 1_000_000) {
                throw new IllegalArgumentException("Contagem de registos inválida");
            }

            List<Registo> registos = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                long lsn = in.readLong();
                long instanteMs = in.readLong();
                String autor = readString(in);
                String canal = readString(in);
                String texto = readString(in);
                registos.add(new Registo(lsn, Instant.ofEpochMilli(instanteMs), autor, canal, texto));
            }
            return registos;
        } catch (IOException e) {
            throw new IllegalArgumentException("Chunk com formato binário inválido", e);
        }
    }

    private static byte[] serializeRegistos(List<Registo> registos) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(registos.size());
            for (Registo r : registos) {
                out.writeLong(r.getLsn());
                out.writeLong(r.getInstante().toEpochMilli());
                writeString(out, r.getAutor());
                writeString(out, r.getCanal());
                writeString(out, r.getTexto());
            }
        }
        return bytes.toByteArray();
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(data.length);
        out.write(data);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > 16 * 1024 * 1024) {
            throw new IOException("Comprimento de string inválido");
        }
        byte[] data = new byte[length];
        in.readFully(data);
        return new String(data, StandardCharsets.UTF_8);
    }
}
