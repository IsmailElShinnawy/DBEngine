import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Config {
	Properties configFile;

	public Config(String strPath) {
		File config = new File(strPath);
		configFile = new Properties();
			
		FileInputStream fis;
		try {
			fis = new FileInputStream(config);
			configFile.load(fis);
			fis.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getProperty(String key) {
		String value = this.configFile.getProperty(key);
		return value;
	}
}