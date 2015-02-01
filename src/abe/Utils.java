package abe;

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
	
	public static int editDistance(String word1, String word2) {
		int len1 = word1.length();
		int len2 = word2.length();
	 
		// len1+1, len2+1, because finally return dp[len1][len2]
		int[][] dp = new int[len1 + 1][len2 + 1];
	 
		for (int i = 0; i <= len1; i++) 
			dp[i][0] = i;
	 
		for (int j = 0; j <= len2; j++)
			dp[0][j] = j;
	 
		//iterate though, and check last char
		for (int i = 0; i < len1; i++) {
			char c1 = word1.charAt(i);
			for (int j = 0; j < len2; j++) {
				char c2 = word2.charAt(j);
	 
				//if last two chars equal
				if (c1 == c2) {
					//update dp value for +1 length
					dp[i + 1][j + 1] = dp[i][j];
				} else {
					int replace = dp[i][j] + 1;
					int insert = dp[i][j + 1] + 1;
					int delete = dp[i + 1][j] + 1;
	 
					int min = replace > insert ? insert : replace;
					min = delete > min ? min : delete;
					dp[i + 1][j + 1] = min;
				}
			}
		}
	 
		return dp[len1][len2];
	}
}
