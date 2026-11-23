package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

public final class ChunkCrypto {
    public static final int KEY_VERSION = 1;
    private static final byte[] HKDF_INFO = "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AAD_PREFIX = "tpf-v1".getBytes(StandardCharsets.UTF_8);
    private static final SecureRandom RANDOM = new SecureRandom();

    private ChunkCrypto() {
    }

    public static Sealed seal(byte[] paddedText, byte[] masterKey) {
        validateMasterKey(masterKey);
        byte[] salt = new byte[32];
        byte[] nonce = new byte[12];
        RANDOM.nextBytes(salt);
        RANDOM.nextBytes(nonce);
        byte[] cipherText = encrypt(paddedText, masterKey, salt, nonce, KEY_VERSION);
        String chunkId = sha256Hex(cipherText);
        return new Sealed(chunkId, cipherText, KEY_VERSION, salt, nonce);
    }

    public static Sealed sealPaddedChunk(byte[] paddedText, byte[] masterKey) {
        validateMasterKey(masterKey);
        if (paddedText.length % Wire.PAD_BYTES != 0) {
            throw new IllegalArgumentException("Texto do chunk não está alinhado");
        }
        if (paddedText.length < Wire.PAD_BYTES) {
            throw new IllegalArgumentException("Texto do chunk demasiado curto");
        }
        return seal(paddedText, masterKey);
    }

    public static byte[] open(Sealed sealed, byte[] masterKey) {
        validateMasterKey(masterKey);
        if (sealed == null) {
            throw new IllegalArgumentException("Sealed nulo");
        }

        byte[] cipherText = sealed.getCipherText();
        String actualId = sha256Hex(cipherText);
        if (!MessageDigest.isEqual(actualId.getBytes(StandardCharsets.US_ASCII),
                sealed.getChunkId().getBytes(StandardCharsets.US_ASCII))) {
            throw new IllegalArgumentException("Falha na identificação do chunk");
        }
        if (sealed.getKeyVersion() != KEY_VERSION) {
            throw new IllegalArgumentException("Versão da chave não suportada");
        }
        if (sealed.getSalt().length != 32) {
            throw new IllegalArgumentException("Salt inválido");
        }
        if (sealed.getNonce().length != 12) {
            throw new IllegalArgumentException("Nonce inválido");
        }

        return decrypt(cipherText, masterKey, sealed.getSalt(), sealed.getNonce(), sealed.getKeyVersion());
    }

    public static Sealed sealWithParameters(byte[] paddedText, byte[] masterKey,
                                             byte[] salt, byte[] nonce, int keyVersion) {
        validateMasterKey(masterKey);
        if (salt.length != 32) throw new IllegalArgumentException("Salt inválido");
        if (nonce.length != 12) throw new IllegalArgumentException("Nonce inválido");
        byte[] cipherText = encrypt(paddedText, masterKey, salt, nonce, keyVersion);
        return new Sealed(sha256Hex(cipherText), cipherText, keyVersion, salt, nonce);
    }

    public static byte[] encrypt(byte[] plaintext, byte[] masterKey,
                                 byte[] salt, byte[] nonce, int keyVersion) {
        validateMasterKey(masterKey);
        if (salt.length != 32) throw new IllegalArgumentException("Salt inválido");
        if (nonce.length != 12) throw new IllegalArgumentException("Nonce inválido");
        try {
            byte[] dek = hkdfSha256(masterKey, salt, HKDF_INFO);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE,
                    new SecretKeySpec(dek, "AES"),
                    new GCMParameterSpec(128, nonce));
            cipher.updateAAD(buildAad(keyVersion, salt, nonce));
            return cipher.doFinal(plaintext);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Falha a cifrar o chunk", e);
        }
    }

    public static byte[] decrypt(byte[] cipherText, byte[] masterKey,
                                 byte[] salt, byte[] nonce, int keyVersion) {
        validateMasterKey(masterKey);
        if (salt.length != 32) throw new IllegalArgumentException("Salt inválido");
        if (nonce.length != 12) throw new IllegalArgumentException("Nonce inválido");
        try {
            byte[] dek = hkdfSha256(masterKey, salt, HKDF_INFO);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE,
                    new SecretKeySpec(dek, "AES"),
                    new GCMParameterSpec(128, nonce));
            cipher.updateAAD(buildAad(keyVersion, salt, nonce));
            return cipher.doFinal(cipherText);
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("Falha de autenticação do chunk", e);
        }
    }

    public static byte[] hkdfSha256(byte[] masterKey, byte[] salt, byte[] info) {
        try {
            Mac hmac = Mac.getInstance("HmacSHA256");
            hmac.init(new SecretKeySpec(salt, "HmacSHA256"));
            byte[] prk = hmac.doFinal(masterKey);

            hmac.init(new SecretKeySpec(prk, "HmacSHA256"));
            hmac.update(info);
            hmac.update((byte) 0x01);
            return hmac.doFinal();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Falha no HKDF-SHA256", e);
        }
    }

    public static byte[] buildAad(int keyVersion, byte[] salt, byte[] nonce) {
        if (salt.length != 32) throw new IllegalArgumentException("Salt inválido");
        if (nonce.length != 12) throw new IllegalArgumentException("Nonce inválido");
        ByteBuffer aad = ByteBuffer.allocate(AAD_PREFIX.length + 4 + salt.length + nonce.length);
        aad.put(AAD_PREFIX);
        aad.putInt(keyVersion);
        aad.put(salt);
        aad.put(nonce);
        return aad.array();
    }

    public static String sha256Hex(byte[] data) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                String value = Integer.toHexString(Byte.toUnsignedInt(b));
                if (value.length() == 1) hex.append('0');
                hex.append(value);
            }
            return hex.toString();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 indisponível", e);
        }
    }

    public static String encodeBase64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    public static byte[] decodeBase64(String data) {
        return Base64.getDecoder().decode(data);
    }

    private static void validateMasterKey(byte[] masterKey) {
        if (masterKey == null || masterKey.length != 32) {
            throw new IllegalArgumentException("master.key deve ter 32 bytes");
        }
    }
}
