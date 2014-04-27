package abe;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import abe.core.RIPEMD160Digest;

public class Utils {
	/**
	 * Calculates RIPEMD160(SHA256(input)). This is used in Address
	 * calculations.
	 */
	public static byte[] sha256hash160(byte[] input) {
		try {
			byte[] sha256 = MessageDigest.getInstance("SHA-256").digest(input);
			RIPEMD160Digest digest = new RIPEMD160Digest();
			digest.update(sha256, 0, sha256.length);
			byte[] out = new byte[20];
			digest.doFinal(out, 0);
			return out;
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e); // Cannot happen.
		}
	}

	/** Returns the given byte array hex encoded. */
	public static String bytesToHexString(byte[] bytes) {
		StringBuffer buf = new StringBuffer(bytes.length * 2);
		for (byte b : bytes) {
			String s = Integer.toString(0xFF & b, 16);
			if (s.length() < 2)
				buf.append('0');
			buf.append(s);
		}
		return buf.toString();
	}
}
