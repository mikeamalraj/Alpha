package etl.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import etl.config.Configurations;
import etl.config.Constants;

public class DataMapper {
	public String getUsageData(String fileLocation, String columnFileName, String data, String delimiter, String columnDef)
			throws Exception {
		Map<String, String> dataMap = getObject(data, delimiter,
				new Configurations().getConfigurations(fileLocation, columnFileName));
		// String usageType = dataMap.get("Usage_Type");
		// String roamingInd = dataMap.get("Overseas_Ind");
		// String dataUsed = dataMap.get("Usage_Count");
		// switch (usageType) {
		// case "V":
		// if (roamingInd.equals("Y"))// N domestic or Y roaming
		// dataMap.put("voiceRoamingCounter", dataUsed);
		// else {
		// dataMap.put("voiceDomesticCounter", dataUsed);
		// }
		// break;
		// case "D":// data doesn't have a roaming option
		// dataMap.put("dataCounter", dataUsed);
		// break;
		// case "S":
		// if (roamingInd.equals("Y")) {
		// dataMap.put("smsRoamingCounter", dataUsed + 1);
		// } else {
		// dataMap.put("smsDomesticCounter", dataUsed + 1);
		// }
		// break;
		// default:
		// System.err.println("Usage Type " + usageType + "not found. File
		// format might have changed?");
		// break;
		// }
		return getString(dataMap, columnDef, data, delimiter);
	}

	private String getString(Map<String, String> dataMap, String columnDef, String data, String delimiter) {
		String[] columnData = data.split(Constants.delimiters.get(delimiter.trim()));
		String[] columnDefArray = columnDef.split("\\|");
		String values = Constants.EMPTY;
		for (int i = 0; i < columnDefArray.length; i++) {
			if (columnData.length > i && (columnData[i] != null)) {
				String[] info = columnDefArray[i].trim().split(" ");
				
				if (info[1].contains("date")) {
					if (columnData[i].contains("/")) {
						values = values + "STR_TO_DATE('" + columnData[i] + "'," + Constants.convertions.get("date1")
								+ ")";
					} else {
						values = values + "STR_TO_DATE('" + columnData[i] + "'," + Constants.convertions.get("date2")
								+ ")";
					}
				} else if (info[1].contains("time")) {
					values = values + "STR_TO_DATE('" + columnData[i] + "'," + Constants.convertions.get("time") + ")";
				} else if (!info[1].contains("int") && !info[1].contains("double")) {
					values = values + "'" + columnData[i].replace("'", "''") + "'";
				} else {
					if (columnData[i].equals(Constants.EMPTY)) {
						values = values + "0";
					} else {
						values = values + columnData[i];
					}
				}
			} else {
				values = values + "''";
			}
			if (i != columnDefArray.length - 1) {
				values = values + Constants.DELIMITER_COMMA;
			}
		}
		return values;
	}

	private Map<String, String> getObject(String data, String delimiter, Properties tableDef) {
		// userName : Jon
		Map<String, String> dataMap = new HashMap<String, String>();
		String[] dataArray = data.split(Constants.delimiters.get(delimiter.trim()));
		int i = 0;
		for (Entry<Object, Object> entry : tableDef.entrySet()) {
			if (dataArray.length <= i) {
				dataMap.put(entry.getKey().toString(), dataArray[Integer.parseInt((String) entry.getValue()) - 1]);
			} else {
				dataMap.put(entry.getKey().toString(), "''");
			}
			i++;
		}
		return dataMap;
	}
}
