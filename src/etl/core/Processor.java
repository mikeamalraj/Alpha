package etl.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import etl.config.Configurations;
import etl.config.Constants;

public class Processor {
	public static void main(String args[]) throws Exception {
		Processor p = new Processor();
		p.execute("201510");
		p.execute();
	}

	private void execute() throws Exception {
		// execute(Constants.getCurrentMonth());
	}

	private void execute(String month) throws Exception {
		String CURRENT_MONTH = Constants.getCurrentMonth();
		System.out.println("CURRENT_MONTH [" + CURRENT_MONTH + "], EXE_MONTH [" + month + "]");
		if (CURRENT_MONTH.equals(month)) {
			System.out.println("Current Month Jobs not ready. ");
			System.exit(0);
		}
		Connection connection = null;
		try {
			Configurations configurations = new Configurations();
			Properties config = configurations.getConfigurations(Constants.CONFIG_FILE);
			connection = getConnection(configurations.getConfigurations(config.getProperty(Constants.DB_FILE)));
			// Monthly Load
			month = month != null ? month : config.getProperty(Constants.LOADED_MONTH);
			String[] monthlyTask = null;
			if (null != config.get(Constants.MONTHLY_TASK)) {
				monthlyTask = config.get(Constants.MONTHLY_TASK).toString().split(Constants.DELIMITER_COMMA);
			}
			if (monthlyTask != null && monthlyTask.length > 0) {
				for (String mT : monthlyTask) {
					int returnCode = -1;
					try {
						returnCode = processTask(config.get("rddLocation").toString(), mT, config, true, connection,
								month);
					} catch (Exception e) {
						throw e;
					}
					if (returnCode != 0) {
						throw new Exception("Invalid return Code [" + returnCode + "] for task [" + mT + "]");
					}
				}
			}
			// Daily Load
			String[] dailyTask = null;
			if (null != config.get(Constants.DAILY_TASK)) {
				dailyTask = config.get(Constants.DAILY_TASK).toString().split(Constants.DELIMITER_COMMA);
			}
			if (dailyTask != null && dailyTask.length > 0) {
				for (String dT : dailyTask) {
					int returnCode = -1;
					try {
						returnCode = processTask(config.get("dailyLocation").toString(), dT, config, false, connection,
								Constants.EMPTY);
					} catch (Exception e) {
						throw e;
					}
					if (returnCode != 0) {
						throw new Exception("Invalid return Code [" + returnCode + "] for task [" + dT + "]");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// connection.commit();
			connection.close();
		}
		System.out.println("Completed ... ");
	}

	private int processTask(String location, String taskName, Properties config, boolean isMonthly,
			Connection connection, String month) throws Exception {
		String simpleFileName = config.get(taskName).toString();
		String fileName = location + simpleFileName;
		int returnCode = 0;
		if (isMonthly) {
			fileName = fileName + month + config.get("fileExtention");
			simpleFileName = simpleFileName + month;
		}
		simpleFileName += config.get("fileExtention");
		if (isMonthlyLoadCompleted()) { // monthly load 1 time or already done.
			return 0;
		}
		int i = processfile(taskName, config, isMonthly ? fileName : location + simpleFileName, connection, isMonthly);
		System.out.println("Processed : " + fileName + ":  RC [" + i + "]");
		if (i == 0) {
			boolean isFileMoved = moveFile(location, simpleFileName);
			System.out.println("File Moved : " + (isFileMoved ? "success" : "failed"));
			if (i != 0) {
				return 1;
			}
		} else {
			System.out.println("File Error: " + fileName);
			return 1;
		}
		return returnCode;
	}

	private int processfile(String taskName, Properties config, String fileName, Connection connection,
			boolean isMonthly) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(fileName));
			String line = Constants.EMPTY;
			Configurations conf = new Configurations();
			Properties tableProperties = conf.getConfigurations(Constants.TABLE_FILE);
			String columnName = tableProperties.getProperty(taskName + "_Columns");
			columnName = columnName.replace("|", ",");
			String create_stmt = "CREATE TABLE if not exists " + tableProperties.getProperty(taskName + "_Name") + " ("
					+ columnName + ")";
			System.out.println("CREATE create_stmt : " + create_stmt);
			connection.prepareStatement(create_stmt).execute();
			System.out.println(create_stmt);
			String tablePropFileName = config.getProperty(taskName + "_Table");
			System.out.println(taskName + " :" + tablePropFileName);
			int lineNumber = 0;
			while ((line = br.readLine()) != null) {
				if (isMonthly && lineNumber == 0) {
					lineNumber++;
					continue;
				}
				String query = "INSERT INTO " + tableProperties.getProperty(taskName + "_Name") + " values ("
						+ dumpData(config.getProperty(taskName + "_Table"), line,
								tableProperties.getProperty(taskName + "_Delimiter"),
								tableProperties.getProperty(taskName + "_Columns"))
						+ ")";
				System.out.println("INSERT query : " + query);
				connection.prepareStatement(query).execute();
				System.out.println("INSERT Completed");
				if (true) {
					// throw new Exception("Break at 1");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	private String dumpData(String columnFile, String data, String delimiter, String columnDef) throws Exception {
		String values = new DataMapper().getUsageData(columnFile, data, delimiter, columnDef);
		return values;
	}

	private boolean isMonthlyLoadCompleted() {
		// TODO : write logic to compare the already done load and current month
		// load
		return false;
	}

	private boolean moveFile(String fileDir, String fileName) {
		File afile = new File(fileDir + "\\" + fileName);
		String[] fileInfo = fileName.split("\\.");
		SimpleDateFormat date = new SimpleDateFormat("_yyyyMMddHHmmss");
		if (afile.renameTo(new File(fileDir + "\\Processed\\" + fileInfo[0] + date.format(new Date()) + ".txt"))) {
			return true;
		}
		return false;
	}

	public Connection getConnection(Properties properties) throws Exception {
		Class.forName((String) properties.get("driver"));
		Connection connection = null;
		connection = DriverManager.getConnection("jdbc:mysql://" + properties.get(Constants.IP) + ":"
				+ properties.get(Constants.PORT) + "/" + properties.get(Constants.DB) + "?user="
				+ properties.get(Constants.USER) + "&password=" + properties.get(Constants.PASSWORD));
		System.out.println("Connected Sucessfully to " + properties.get(Constants.IP));
		return connection;
	}
}
