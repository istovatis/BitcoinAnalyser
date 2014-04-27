package parser;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import org.junit.Test;

import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.TextPage;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

/**
 * This is a general webpage parser.
 * 
 * @author istovatis
 * 
 */
public class WebPageParser {
	
	@Test
	public static List<?>  parseWebPage(String webPage, String xpath) {
		final WebClient webClient = new WebClient();
		webClient.getOptions().setCssEnabled(false);
		webClient.getOptions().setJavaScriptEnabled(false);
		HtmlPage page = null;
		try {
			page = webClient.getPage(webPage);
		} catch (FailingHttpStatusCodeException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<?> list = page.getByXPath(xpath);
		webClient.closeAllWindows();
		return list;
	}
	
	public static String parseWholeTextPage(String webPage) {
		final WebClient webClient = new WebClient();
		webClient.getOptions().setCssEnabled(false);
		webClient.getOptions().setJavaScriptEnabled(false);
		TextPage page = null;
		try {
			page = webClient.getPage(webPage);
		} catch (FailingHttpStatusCodeException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		webClient.closeAllWindows();
		return page.getContent();
	}
	
}
