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
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Processor {
	/** The Logger object is initialized here(in order to create log file for Processor class) based on Processor class .*/  
	static Logger logger = Logger.getLogger(Processor.class);
	
	/** The message method prints information about the command line arguments that needs to be passed while running the program.*/ 
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

	/** The main method gets the command line arguments informations. If all needed informations are passed then it will call execute method for processing. Otherwise it will call message method */ 
	public static void main(String args[]) throws Exception {
		
		/** If no argument is passed - call message method */
		if (args.length == 0) {
			message();
		} else {
			/** Initializing all variables */
			boolean isMonthly = false;
			boolean isDaily = false;
			String resourceFileLocation = Constants.EMPTY;
			String inputMonth = Constants.EMPTY;
			String job = Constants.EMPTY;
			String sourceDirectory = Constants.EMPTY;
			
			/** Assigning command line information to corresponding variable*/
			for (int i = 0; i < args.length; i++) {
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
			
			/** At a time either Daily or Monthly data will be processed - if configuration files location is provided. Otherwise message method will be called */
			if ((isMonthly || isDaily) && !(isMonthly && isDaily) && !resourceFileLocation.isEmpty()) {
				Processor p = new Processor();
				
				/** Configuring Log4j properties */
				PropertyConfigurator.configure( resourceFileLocation + "Log4j.properties");
				if (isMonthly && inputMonth.isEmpty()) {
					inputMonth = Constants.getCurrentMonth();
				}
				if (job.isEmpty()) {
					job = "All";
				}
				logger.info( (isMonthly ? "************************** Monthly Job is executed **************************" : "************************** Daily Job is executed **************************"));
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
	
	/** The execute method reads configuration files, basic properties and calls processTask method for further execution.*/
	
	private void execute(boolean isMonthly, String resourceFileLocation, String inputMonth, String job,
			String sourceDirectory) throws Exception {
		Connection connection = null;
		try {
			Configurations configurations = new Configurations();
			/** Reading configurations from config.properties file */
			Properties config = configurations.getConfigurations(resourceFileLocation, Constants.CONFIG_FILE);
			
			/** Reading configurations from localDB.properties file */
			connection = getConnection(
					configurations.getConfigurations(resourceFileLocation, config.getProperty(Constants.DB_FILE)));
			
			/** The monthly based load is performed here.*/
			if (isMonthly) {
				inputMonth = inputMonth != null ? inputMonth : config.getProperty(Constants.LOADED_MONTH);
				String[] monthlyTask = null;
				
				/** Getting the information of what all are the monthly job(s) to be performed */
				if (!job.isEmpty() && !job.equals("All")) {
					monthlyTask = job.split(Constants.DELIMITER_COMMA);
				} else if (null != config.get(Constants.MONTHLY_TASK)) {
					monthlyTask = config.get(Constants.MONTHLY_TASK).toString().split(Constants.DELIMITER_COMMA);
				}
				
				/** If at least one monthly job is available then below code will be executed. */
				if (monthlyTask != null && monthlyTask.length > 0) {
					
					/** Calling processTask method in loop for each type of monthly task. */
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
			/** The daily based load is performed here.*/
			else {
				String[] dailyTask = null;
				if (!job.isEmpty() && !job.equals("All")) {
					dailyTask = job.split(Constants.DELIMITER_COMMA);
				} else if (null != config.get(Constants.DAILY_TASK)) {
					dailyTask = config.get(Constants.DAILY_TASK).toString().split(Constants.DELIMITER_COMMA);
				}
				if (dailyTask != null && dailyTask.length > 0) {
					
					/** Calling processTask method in loop for each type of daily task. */
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

	/** This processTask method reads all files in the given directory. If matching file is found in that directory corresponding to the task then another processTask method will be called for processing the matching file. */
	private int processTask(String resourceFileLocation, String sourceDirectory, String taskName, Properties config,
			boolean isMonthly, Connection connection, String month) throws Exception {
		File dir = new File(sourceDirectory);
		int i = 0;
		for (File file : dir.listFiles()) {
			if (file.getName().startsWith(config.get(taskName).toString())) {
				i = processTask(resourceFileLocation, sourceDirectory, taskName, config, isMonthly, connection, month,
						file.getName());	
			}
		}
		return i;
	}

	/** This processTask method calls the processfile method for reading the content of the file. If file has been read successfully then it will call moveFile method to move the processed file into sub directory (Processed)  */
	private int processTask(String resourceFileLocation, String sourceDirectory, String taskName, Properties config,
			boolean isMonthly, Connection connection, String month, String fileName) throws Exception {
		System.out.println("processTask : " + taskName);
		int returnCode = 0;
		
		/** Calling the processfile method for reading the content of the file and writing into database */
		int i = processfile(resourceFileLocation, sourceDirectory, taskName, config, fileName, connection, isMonthly);
		System.out.println("Processed : " + fileName + ":  RC [" + i + "]");
		
		/** If file has been processed successfully then the below code will move the processed file into sub directory. */
		if (i == 0) {
			logger.info( fileName + " file processed successfully!");
			boolean isFileMoved = moveFile(sourceDirectory, fileName);
			System.out.println("File Moved : " + (isFileMoved ? "success" : "failed"));
			logger.info( fileName + " file : " + (isFileMoved ? " moved successfully!" : "move failed!"));
			if (!isFileMoved) {
				return 1;
			}
		
		/** If file is not processed successfully then the below code will print the error type. */
		} else if (i != 4) {
			System.out.println(": File Error" + fileName);
			logger.error( fileName + " file not processed! Error in file" );
			return 1;
		}
		return returnCode;
	}

	/** The processfile method reads the file and inserts the file content into corresponding table. */
	
	private int processfile(String resourceFileLocation, String sourceDirectory, String taskName, Properties config, String fileName,
			Connection connection, boolean isMonthly) {
		String absoluteFileName= sourceDirectory + fileName;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(absoluteFileName));
			String line = Constants.EMPTY;
			Configurations conf = new Configurations();
			
			/** Reading configurations from table.properties file and fetching columns corresponding to table*/
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
			
			/** Reading each line in loop from the file and inserting the same into the table*/
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
		/** If not able to read and insert the file content into table then below code will be executed.*/
		} catch (java.io.FileNotFoundException e) {
			System.out.println("File skipped (Not Found) : " + fileName);
			logger.error( fileName + " file not processed! File not found!" );
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

	/** The dumpData method will return records (or columns) corresponding to the each line in the file. */
	private String dumpData(String fileLocation, String columnFile, String data, String delimiter, String columnDef)
			throws Exception {
				return new DataMapper().getUsageData(fileLocation, columnFile, data, delimiter, columnDef);
	}
	
	/** The moveFile moves the processed file into sub (Processed) folder. */
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
	
	/** The getConnection method will connect to the database. */
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
