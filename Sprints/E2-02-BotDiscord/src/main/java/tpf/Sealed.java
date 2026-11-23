package tpf;

import java.util.Arrays;

public final class Sealed {
    private final String chunkId;
    private final byte[] cipherText;
    private final int keyVersion;
    private final byte[] salt;
    private final byte[] nonce;

    public Sealed(String chunkId, byte[] cipherText, int keyVersion, byte[] salt, byte[] nonce) {
        this.chunkId = chunkId;
        this.cipherText = cipherText.clone();
        this.keyVersion = keyVersion;
        this.salt = salt.clone();
        this.nonce = nonce.clone();
    }

    public String getChunkId() {
        return chunkId;
    }

    public byte[] getCipherText() {
        return cipherText.clone();
    }

    public int getKeyVersion() {
        return keyVersion;
    }

    public byte[] getSalt() {
        return salt.clone();
    }

    public byte[] getNonce() {
        return nonce.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Sealed other)) return false;
        return keyVersion == other.keyVersion
                && chunkId.equals(other.chunkId)
                && Arrays.equals(cipherText, other.cipherText)
                && Arrays.equals(salt, other.salt)
                && Arrays.equals(nonce, other.nonce);
    }

    @Override
    public int hashCode() {
        int result = chunkId.hashCode();
        result = 31 * result + Arrays.hashCode(cipherText);
        result = 31 * result + keyVersion;
        result = 31 * result + Arrays.hashCode(salt);
        result = 31 * result + Arrays.hashCode(nonce);
        return result;
    }
}
