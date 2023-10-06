//=================================================================================================
///  @file Settings.java
///
///  Definition of the JSON connection settings
///
///  Copyright (C) 2015 Simba Technologies Incorporated
//=================================================================================================

package com.useready.rester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import org.apache.http.impl.client.CloseableHttpClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simba.dsi.dataengine.utilities.ColumnMetadata;
import com.simba.support.ILogger;
import com.simba.support.LogUtilities;

/**
 * Connection settings.
 */
public class Settings {
	/*
	 * Instance variable(s)
	 * ========================================================================
	 */

	/**
	 * The full path to the JSON DBF files.
	 */

	private ILogger m_logger;
	public String m_dbfPath = "";
	public String m_nadia = "";
	public String m_username = "";
	public String token = "";
	public String m_host = "";
	public JsonNode node;
	public String response;
	/**
	 * ̥ The default maximum string column size.
	 */
	public long m_maxColumnSize = 255;

	/**
	 * Map that will store all tables in the DBF with catalog and schema names.
	 */
	public Map<String, Map<String, List<String>>> m_tableMap;

	public Map<String, JsonNode> m_tablesList = new TreeMap<String, JsonNode>();

	public Map<String, List<String>> m_tablesLists = new TreeMap<String, List<String>>();

	public String m_server = "";

	public String m_config = "";

	public String m_urlsList = "";

	public String m_database = "";

	public String m_databaseOrder = "";

	String m_singleCall = "";

	public String m_schema = "";

	public String m_schemaOrder = "";

	public List<String> m_SchemaName = new ArrayList<>();

	public String m_tables = "";

	public String m_measureDataType = "";

	public JsonNode m_tablesArr;

	public JsonNode m_fieldsArr;
	public JsonNode m_actualNode;

	public String m_fields = "";

	public String m_format = null;

	public String m_baseUrl = "";

	public String m_baseUrlredriect = "";

	public boolean m_tableList = false;

	public boolean m_tableData = false;

	public String m_levels = "";

	public String m_header = "";

	public String m_TableName = "";

	public boolean m_schemalist = false;

	public JsonNode m_schemaLiST = null;

	public boolean tokengen = false;

	public String m_authType;
	public boolean iterate = false;
	public boolean m_queryRun = false;

	public boolean followHTTPRedirect = false;
	public boolean nodeset = false;
	public CloseableHttpClient httpclient = null;

	public Map<String, JsonNode> m_Tables = new TreeMap<String, JsonNode>();
	public Map<String, JsonNode> m_TablesData = new TreeMap<String, JsonNode>();
	// public JsonNode m_rootNode;

	public JsonNode m_rootNode;
	public List<JsonNode> m_rootNodeList = new ArrayList<JsonNode>();

	public List<JsonNode> m_rootNodeTableList = new ArrayList<JsonNode>();
	public Subject subject;

	public List<String> selectedColumns = new ArrayList<String>();

	public Map<String, List<String>> displayNameComb = new TreeMap<String, List<String>>();

	public ArrayList<ColumnMetadata> m_metadataList = new ArrayList<ColumnMetadata>();
	
	public JsonNode m_DataNode =null;
	
	public List<Short> m_listOfColumnTypesList = new ArrayList<Short>();

	public Map<String, Integer> m_columnIndexData = new HashMap<>();
	
	public Map<String, String> m_columnData = new HashMap<>();

	public String str;

	public String m_filter = "";
	public List<String> originalColumnNames = new ArrayList<String>();

	public JsonNode m_tableFields;

	public void settingsJson(JsonNode node, ILogger iLogger) {
		try {
			this.m_logger = iLogger;
			this.m_authType = node.get("authenticationType").toString().replace("\"", "");
			LogUtilities.logDebug("-authenticationType------- " + this.followHTTPRedirect, iLogger);
			this.followHTTPRedirect = node.get("followHTTPRedirect").asBoolean();
			LogUtilities.logDebug("-followHTTPRedirect setting-------- " + this.followHTTPRedirect, iLogger);

			// this.m_host = node.get("host").toString().replace("\"", "");
			// this.m_baseUrl = node.get("baseUrl").toString().replace("\"", "");
			this.m_baseUrlredriect = node.get("baseUrl").toString().replace("\"", "");
			// this.m_database = node.get("database").toString().replace("\"", "");
			if (node.get("database") != null) {
				String database = node.get("database").toString().replace("\"", "");
				/// JSON /RAW Data
				if (database.contains("{")) {
					JsonNode nodes = node.get("database");
					this.m_database = nodes.get("url").toString().replace("\"", "");
					if (nodes.has("order")) {
						this.m_databaseOrder = nodes.get("order").toString().replace("\"", "");
					}
					LogUtilities.logDebug("--m_databaseOrder---------" + this.m_databaseOrder, m_logger);

				} else {
					this.m_database = node.get("database").toString().replace("\"", "");
				}
			}
			// this.m_singleCall= node.get("singleApi").toString().replace("\"", "");
			if (node.get("schema") != null) {
				String schema = node.get("schema").toString().replace("\"", "");
				/// JSON /RAW Data
				if (schema.contains("{")) {
					JsonNode nodes = node.get("schema");
					this.m_schema = nodes.get("url").toString().replace("\"", "");
					if (nodes.has("order")) {
						this.m_schemaOrder = nodes.get("order").toString().replace("\"", "");
					}
					this.m_SchemaName = getName(nodes, "colName");
//				LogUtilities.logDebug("--m_SchemaName---------" + m_schema + "---" + m_SchemaName, m_logger);

				} else {
					this.m_schema = node.get("schema").toString().replace("\"", "");
				}
			}

			// this.m_schema=node.get("schema").toString().replace("\"", "");
			if (node.get("tableFields") != null && node.get("tableFields").toString().contains("[")) {
				this.m_tablesArr = node.get("tableFields");
			} else {
				if (node.has("tableFields")) {
					this.m_tables = node.get("tableFields").toString().replace("\"", "");
				}

			}
			if (node.get("fields") != null && node.get("fields").toString().contains("{")) {
				this.m_fieldsArr = node.get("fields");
			} else {
				if (node.has("fields")) {
					this.m_fields = node.get("fields").toString().replace("\"", "");
				}

			}
			if (node.get("format") != null) {
				this.m_format = node.get("format").toString().replace("\"", "");
			}
			// this.m_fields = node.get("fields").toString();
			if (node.get("tableName") != null) {
				this.m_TableName = node.get("tableName").toString().replace("\"", "");
			}

			if (node.get("levels") != null) {
				this.m_levels = node.get("levels").toString().replace("\"", "");
			}
		} catch (Exception e) {
			LogUtilities.logError("-Exception---- " + e.getMessage(), iLogger);
			e.printStackTrace();
		}
	}

	/**
	 * @param nodes
	 * @param nameToGet
	 * @return
	 */
	private List<String> getName(JsonNode nodes, String nameToGet) {
		return extractPlaceholders(nodes.get(nameToGet).toString().replace("\"", ""));
	}

	public List<String> extractPlaceholders(String colName) {
		List<String> placeholders = new ArrayList<>();
		Pattern pattern = Pattern.compile("\\{(.*?)\\}"); // Regular expression to match text within curly braces

		Matcher matcher = pattern.matcher(colName);
		while (matcher.find()) {
			String placeholder = matcher.group(1); // Get the text within curly braces
			placeholders.add(placeholder);
		}

		return placeholders;
	}

	public Settings() {
		this.m_logger = null;
		// TODO Auto-generated constructor stub
	}

	public String getM_database() {
		return m_database;
	}

	public void setM_database(String m_database) {
		this.m_database = m_database;
	}

	public String getM_schema() {
		return m_schema;
	}

	public void setM_schema(String m_schema) {
		this.m_schema = m_schema;
	}

	public String getM_tables() {
		return m_tables;
	}

	public void setM_tables(String m_tables) {
		this.m_tables = m_tables;
	}

	public String getM_fields() {
		return m_fields;
	}

	public void setM_fields(String m_fields) {
		this.m_fields = m_fields;
	}

	public ILogger getM_logger() {
		return m_logger;
	}

	public void setM_logger(ILogger m_logger) {
		this.m_logger = m_logger;
	}

	public String getM_header() {
		return m_header;
	}

	public void setM_header(String m_header) {
		this.m_header = m_header;
	}

	public Map<String, Map<String, List<String>>> getM_tableMap() {
		return m_tableMap;
	}

	public void setM_tableMap(Map<String, Map<String, List<String>>> m_tableMap) {
		this.m_tableMap = m_tableMap;
	}

	public JsonNode getNode() {
		return node;
	}

	public void setNode(JsonNode node) {
		this.node = node;
	}

	public List<String> getSelectedColumns() {
		return selectedColumns;
	}

	public void setSelectedColumns(List<String> selectedColumns) {
		this.selectedColumns = selectedColumns;
	}

}
