import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import argo.jdom.JdomParser;
import argo.jdom.JsonNode;
import argo.jdom.JsonRootNode;
import argo.jdom.JsonStringNode;
import argo.saj.InvalidSyntaxException;

public class Scraper {
	private URL url;
	private HttpURLConnection connection = null;
	private String response;
	private static final JdomParser jsonParser = new JdomParser();
	private JsonRootNode json;
	private static Map<JsonStringNode, JsonNode> map;		// Map created by parsing the json

	public static Map<JsonStringNode, JsonNode> getMap() {
		return map;
	}

	public static void setMap(Map<JsonStringNode, JsonNode> map) {
		Scraper.map = map;
	}

	public String excutePost(String targetURL, String urlParameters) {
		try {
			// Create connection
			url = new URL(targetURL);
			connection = (HttpURLConnection) url.openConnection();

			connection.setRequestProperty("Accept", "application/json");
			connection.setRequestProperty("charset", "utf-8");
			connection.setRequestMethod("GET");

			connection.setConnectTimeout(30000);
			connection.setReadTimeout(30000);

			connection.setUseCaches(false);
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.connect();

			if (connection.getResponseCode() == 200)
				System.out.println("Connection established");
			// System.out.println(IOUtils.toString(
			// connection.getInputStream(), "UTF-8"));
			else if (connection.getResponseCode() == 500
					&& (connection.getContentType() == null || connection
							.getContentType().equals("text/plain")))
				throw new Exception(
						"Error From Server: "
								+ IOUtils.toString(connection.getErrorStream(),
										"UTF-8"));
			else
				throw new Exception("Unknown response from server");

			// Get Response
			InputStream is = connection.getInputStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(is));
			String line;
			StringBuffer resp = new StringBuffer();
			while ((line = rd.readLine()) != null) {
				resp.append(line);
				resp.append('\r');
			}
			rd.close();
			response = resp.toString();
			return response;

		} catch (Exception e) {

			e.printStackTrace();
			return null;

		} finally {

			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	public void parseJson() {
		try {
			json = jsonParser.parse(response);
			map = json.getFields();
		} catch (InvalidSyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}