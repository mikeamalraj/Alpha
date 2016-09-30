package etl.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

public class Configurations {
	public static class ConfigTest {
		public static void main(String args[]) throws IOException {
			Configurations cf = new Configurations();
			Properties p = cf.getConfigurations("config.properties", true);
			System.out.println(" user : " + p.get("user"));
		}
	}

	public Properties getConfigurations(String fileName) throws IOException {
		return getConfigurations(fileName, false);
	}

	public Properties getConfigurations(String fileName, boolean doPrint) throws IOException {
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(fileName);
		Properties properties = new Properties();
		properties.load(inputStream);
		if (doPrint) {
			for (Entry<Object, Object> entry : properties.entrySet()) {
				System.out.println(entry.getKey() + " : " + entry.getValue());
			}
		}
		return properties;
	}
}