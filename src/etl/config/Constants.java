package etl.config;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public final class Constants {
	public static String CONFIG_FILE = "config.properties";
	public static String TABLE_FILE = "table.properties";
	public static String DB_FILE = "db";
	public static String MONTHLY_TASK = "monthly";
	public static String DAILY_TASK = "daily";
	public static String LOADED_MONTH = "loadedMonth";
	public static String USER = "user";
	public static String PASSWORD = "password";
	public static String IP = "ip";
	public static String PORT = "port";
	public static String DB = "database";
	public static String EMPTY = "";
	public static String DELIMITER_COMMA = ",";
	public static Map<String, String> delimiters = new HashMap<String, String>();
	public static Map<String, String> convertions = new HashMap<String, String>();
	static {
		delimiters.put("comma", "\\,");
		delimiters.put("tab", "\\t");
		delimiters.put("pipe", "|");
		convertions.put("date", "'%m/%d/%Y'");
		convertions.put("time", "'%H.%i.%s'");
	}

	public static String getCurrentMonth() {
		DateFormat dateFormat = new SimpleDateFormat("YYYYMM");
		return dateFormat.format(new Date());
	}
}
