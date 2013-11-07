
public class Config {
	
	private static boolean DBIntegration;	// This is true if you want to integrate with database
	private static boolean dublicatesIntegration; // This is true if you want to integrate with dublicate data
	
	public static boolean isDublicatesIntegration() {
		return dublicatesIntegration;
	}

	public static void setDublicatesIntegration(boolean dublicatesIntegration) {
		Config.dublicatesIntegration = dublicatesIntegration;
	}

	public static boolean isDBIntegration() {
		return DBIntegration;
	}

	public static void setDBIntegration(boolean dBIntegration) {
		DBIntegration = dBIntegration;
	}
}
