package etl.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import etl.config.Configurations;
import etl.config.Constants;

public class DataMapper {
	public String getUsageData(String columnFileName, String data, String delimiter, String columnDef)
			throws Exception {
		Map<String, String> dataMap = getObject(data, new Configurations().getConfigurations(columnFileName));
		String usageType = dataMap.get("usageType");
		String roamingInd = dataMap.get("roamingInd");
		String dataUsed = dataMap.get("dataUsed");
		switch (usageType) {
		case "V":
			if (roamingInd.equals("Y"))// N domestic or Y roaming
				dataMap.put("voiceRoamingCounter", dataUsed);
			else {
				dataMap.put("voiceDomesticCounter", dataUsed);
			}
			break;
		case "D":// data doesn't have a roaming option
			dataMap.put("dataCounter", dataUsed);
			break;
		case "S":
			if (roamingInd.equals("Y")) {
				dataMap.put("smsRoamingCounter", dataUsed + 1);
			} else {
				dataMap.put("smsDomesticCounter", dataUsed + 1);
			}
			break;
		default:
			System.err.println("Usage Type " + usageType + "not found.  File format might have changed?");
			break;
		}
		return getString(dataMap, columnDef, data, delimiter);
	}

	private String getString(Map<String, String> dataMap, String columnDef, String data, String delimiter) {
		String[] columnData = data.split(delimiter.trim());
		String[] columnDefArray = columnDef.split(Constants.DELIMITER_COMMA);
		String values = Constants.EMPTY;
		for (int i = 0; i < columnDefArray.length; i++) {
			if (columnDefArray[i].contains("date")) {
				values = values + "STR_TO_DATE('" + columnData[i] + "'," + Constants.convertions.get("date") + ")";
			} else if (columnDefArray[i].contains("time")) {
				values = values + "STR_TO_DATE('" + columnData[i] + "'," + Constants.convertions.get("time") + ")";
			} else if (!columnDefArray[i].contains("int")) {
				values = values + "'" + columnData[i] + "'";
			} else {
				values = values + columnData[i];
			}
			if (i != columnData.length - 1) {
				values = values + Constants.DELIMITER_COMMA;
			}
		}
		return values;
	}

	private Map<String, String> getObject(String data, Properties tableDef) {
		// userName : Jon
		Map<String, String> dataMap = new HashMap<String, String>();
		String[] dataArray = data.split(Constants.DELIMITER_COMMA);
		for (Entry<Object, Object> entry : tableDef.entrySet()) {
			dataMap.put(entry.getKey().toString(), dataArray[Integer.parseInt((String) entry.getValue())]);
		}
		return dataMap;
	}
}
