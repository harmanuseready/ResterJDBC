package com.useready.rester.dataengine;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.simba.support.ILogger;
import com.simba.support.LogUtilities;

public class ExtractColumnName {

	
	public static Map<String, String> extractColumnName(String sqlQuery) {
		Map<String, String> columnNames = new HashMap<>();
		String regexPattern = "AS\\s+\"(.*?)\"|\"Fields\"\\.\"([^\"]+)\" AS \"([^\"]+)\"";
		//String regexPattern = "\"Fields\"\\.\"([^\"]+)\" AS \"([^\"]+)\"";
		Pattern pattern = Pattern.compile(regexPattern);
		Matcher matcher = pattern.matcher(sqlQuery);
		while (matcher.find()) {
			String columnName = matcher.group(1);
			String alias = matcher.group(2);
			if(columnName!=null && ( sqlQuery.contains("CAST") || sqlQuery.contains("EXTRACT"))) {
//				LogUtilities.logDebug("Extact Column Name Map alias: " + alias, iLogger);
//				LogUtilities.logDebug("Extact Column Name Map columnName: " + columnName,iLogger);
				String[] parts = columnName.split(":");
	             
	             if (parts.length >= 2) {
	                  columnName = parts[1];
	                  columnNames.put(columnName, columnName);
	             } 
			}else {
			columnNames.put(alias, alias);
			}
//			LogUtilities.logDebug("Extact Column Name Map alias: " + alias, iLogger);
//			LogUtilities.logDebug("Extact Column Name Map columnName: " + columnName, iLogger);
		}
//		LogUtilities.logDebug("Extact Column Name Map: " + columnNames,iLogger);
		return columnNames;
	}
	
	
	public static String extractAggregationData(String query,String fieldName) {
//		LogUtilities.logDebug("--- Else if" + query, iLogger);
		Pattern pattern = Pattern.compile("([A-Z]+)\\((\"[^\"]+\"\\.\"[^\"]+\")\\) AS \"([^\"]+)\"");
		Matcher matcher = pattern.matcher(query);
		while (matcher.find()) {
			String aggregationFunction = matcher.group(1);
//			LogUtilities.logDebug("--- aggregationFunction::::::::::::::" + aggregationFunction, iLogger);
			String columnName = matcher.group(2).replaceAll("^\"|\"$", "");
			int lastDotIndex = columnName.lastIndexOf('.');
			// Extract the substring after the last dot
			String name = columnName.substring(lastDotIndex + 1);
//			LogUtilities.logDebug("--- extractedPart:::::::::::::::" + name, iLogger);
			name = "\"" + aggregationFunction + "(" + name + ")" + "\"";
			if (fieldName == null) {
				fieldName = name;
			} else {
				fieldName = fieldName + "," + name;
			}
//			LogUtilities.logDebug("--- aggregationFunction---" + name, iLogger);
//			LogUtilities.logDebug("--- str1---" + columnName, iLogger);
		}
		return fieldName;

		
	
	}
}
