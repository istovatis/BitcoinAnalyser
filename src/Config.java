
public class Config {
	
	public static boolean DBIntegration;	//set this argument true if you want to integrate with database

	public static boolean isDBIntegration() {
		return DBIntegration;
	}

	public static void setDBIntegration(boolean dBIntegration) {
		DBIntegration = dBIntegration;
	}
}
