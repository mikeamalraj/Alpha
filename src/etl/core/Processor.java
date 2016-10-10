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
	private static void message() {
		System.out.println("************ ETL Process *************");
		System.out.println("Arguments needs to be valid");
		System.out.println("Monthly Jobs Needs below arguments \n" + "-M for Monthly \n"
				+ "-P 'Location_of_Property_File' & \n " + "-I 'Month_of_Load' (Will take Current Month by Default "
				+ Constants.getCurrentMonth() + ")\n" + "-J 'Job_Name' \n" + "-T 'Target_Directory' \n");
		System.out.println("Daily Jobs Needs below arguments\n" + "-D for Daily \n"
				+ " -P 'Location_of_Property_File' \n" + "-J 'Job_Name' \n" + "-T 'Target_Directory' \n");
		System.out.println(
				"java -jar Alpha_2Mysql.jar -M -P '/home/vmuser1192/Project/Alpha/resources/' -I '201510' -J AWS");
		System.out.println("java -jar Alpha_2Mysql.jar -D -P '/home/vmuser1192/Project/Alpha/resources/' -J VIP");
		System.out.println("************ ETL Process *************");
	}

	public static void main(String args[]) throws Exception {
		if (args.length == 0) {
			message();
		} else {
			boolean isMonthly = false;
			boolean isDaily = false;
			String resourceFileLocation = Constants.EMPTY;
			String inputMonth = Constants.EMPTY;
			String job = Constants.EMPTY;
			String sourceDirectory = Constants.EMPTY;
			for (int i = 0; i < args.length; i++) {
				// System.out.println(i + " = " + args[i]);
				switch (args[i]) {
				case "-M":
				case "-m":
					isMonthly = true;
					break;
				case "-D":
				case "-d":
					isDaily = true;
					break;
				case "-P":
				case "-p":
					resourceFileLocation = args[i + 1];
					break;
				case "-J":
				case "-j":
					job = args[i + 1];
					break;
				case "-I":
				case "-i":
					inputMonth = args[i + 1];
					break;
				case "-s":
				case "-S":
					sourceDirectory = args[i + 1];
					break;
				}
			}
			if ((isMonthly || isDaily) && !(isMonthly && isDaily) && !resourceFileLocation.isEmpty()) {
				Processor p = new Processor();
				if (isMonthly && inputMonth.isEmpty()) {
					inputMonth = Constants.getCurrentMonth();
				}
				if (job.isEmpty()) {
					job = "All";
				}
				System.out.println((isMonthly ? "Monthly" : "Daily") + " Job Details");
				System.out.println("Resource File : " + resourceFileLocation);
				System.out.println("Job Lis: " + job);
				System.out.println((isMonthly ? "Input Month : " + inputMonth : ""));
				p.execute(isMonthly, resourceFileLocation, inputMonth, job, sourceDirectory);
			} else {
				message();
			}
		}
	}

	private void execute(boolean isMonthly, String resourceFileLocation, String inputMonth, String job,
			String sourceDirectory) throws Exception {
		Connection connection = null;
		try {
			Configurations configurations = new Configurations();
			Properties config = configurations.getConfigurations(resourceFileLocation, Constants.CONFIG_FILE);
			connection = getConnection(
					configurations.getConfigurations(resourceFileLocation, config.getProperty(Constants.DB_FILE)));
			// Monthly Load
			if (isMonthly) {
				inputMonth = inputMonth != null ? inputMonth : config.getProperty(Constants.LOADED_MONTH);
				String[] monthlyTask = null;
				if (!job.isEmpty() && !job.equals("All")) {
					monthlyTask = job.split(Constants.DELIMITER_COMMA);
				} else if (null != config.get(Constants.MONTHLY_TASK)) {
					monthlyTask = config.get(Constants.MONTHLY_TASK).toString().split(Constants.DELIMITER_COMMA);
				}
				if (monthlyTask != null && monthlyTask.length > 0) {
					for (String mT : monthlyTask) {
						int returnCode = -1;
						try {
							sourceDirectory = sourceDirectory.isEmpty() ? config.get("rddLocation").toString()
									: sourceDirectory;
							returnCode = processTask(resourceFileLocation, sourceDirectory, mT, config, true,
									connection, inputMonth);
						} catch (Exception e) {
							throw e;
						}
						if (returnCode != 0 && returnCode != 4) {
							throw new Exception("Invalid return Code [" + returnCode + "] for task [" + mT + "]");
						}
					}
				}
			}
			// Daily Load
			else {
				String[] dailyTask = null;
				if (!job.isEmpty() && !job.equals("All")) {
					dailyTask = job.split(Constants.DELIMITER_COMMA);
				} else if (null != config.get(Constants.DAILY_TASK)) {
					dailyTask = config.get(Constants.DAILY_TASK).toString().split(Constants.DELIMITER_COMMA);
				}
				if (dailyTask != null && dailyTask.length > 0) {
					for (String dT : dailyTask) {
						int returnCode = -1;
						try {
							sourceDirectory = sourceDirectory.isEmpty() ? config.get("dailyLocation").toString()
									: sourceDirectory;
							returnCode = processTask(resourceFileLocation, sourceDirectory, dT, config, false,
									connection, Constants.EMPTY);
						} catch (Exception e) {
							throw e;
						}
						if (returnCode != 0 && returnCode != 4) {
							throw new Exception("Invalid return Code [" + returnCode + "] for task [" + dT + "]");
						}
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

	private int processTask(String resourceFileLocation, String sourceDirectory, String taskName, Properties config,
			boolean isMonthly, Connection connection, String month) throws Exception {
		File dir = new File(sourceDirectory);
		int i = 0;
		for (File file : dir.listFiles()) {
			if (file.getName().startsWith(taskName)) {
				i = processTask(resourceFileLocation, sourceDirectory, taskName, config, isMonthly, connection, month,
						file.getName());
			}
		}
		return i;
	}

	private int processTask(String resourceFileLocation, String sourceDirectory, String taskName, Properties config,
			boolean isMonthly, Connection connection, String month, String fileName) throws Exception {
		System.out.println("processTask : " + taskName);
		int returnCode = 0;
		// String simpleFileName = config.get(taskName).toString();
		// String fileName = location + simpleFileName;
		// if (isMonthly) {
		// fileName = fileName + month + config.get("fileExtention");
		// simpleFileName = simpleFileName + month;
		// }
		// simpleFileName += config.get("fileExtention");
		int i = processfile(resourceFileLocation, taskName, config, fileName, connection, isMonthly);
		System.out.println("Processed : " + fileName + ":  RC [" + i + "]");
		if (i == 0) {
			boolean isFileMoved = moveFile(sourceDirectory, fileName);
			System.out.println("File Moved : " + (isFileMoved ? "success" : "failed"));
			if (!isFileMoved) {
				return 1;
			}
		} else if (i != 4) {
			System.out.println("File Error: " + fileName);
			return 1;
		}
		return returnCode;
	}

	private int processfile(String resourceFileLocation, String taskName, Properties config, String fileName,
			Connection connection, boolean isMonthly) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(fileName));
			String line = Constants.EMPTY;
			Configurations conf = new Configurations();
			Properties tableProperties = conf.getConfigurations(resourceFileLocation, Constants.TABLE_FILE);
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
						+ dumpData(resourceFileLocation, config.getProperty(taskName + "_Table"), line,
								tableProperties.getProperty(taskName + "_Delimiter"),
								tableProperties.getProperty(taskName + "_Columns"))
						+ ")";
				System.out.println("INSERT query : " + query);
				connection.prepareStatement(query).execute();
				System.out.println("INSERT Completed");
			}
		} catch (java.io.FileNotFoundException e) {
			System.out.println("File skipped (Not Found) : " + fileName);
			return 4;
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

	private String dumpData(String fileLocation, String columnFile, String data, String delimiter, String columnDef)
			throws Exception {
		return new DataMapper().getUsageData(fileLocation, columnFile, data, delimiter, columnDef);
	}

	private boolean moveFile(String fileDir, String fileName) {
		File afile = new File(fileDir + "/" + fileName);
		String[] fileInfo = fileName.split("/.");
		SimpleDateFormat date = new SimpleDateFormat("_yyyyMMddHHmmss");
		String newName = fileDir + "Processed/" + fileInfo[0] + date.format(new Date()) + ".txt";
		System.out.println("New Processed File Location : " + newName);
		if (afile.renameTo(new File(newName))) {
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
