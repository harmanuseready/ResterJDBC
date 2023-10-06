///  @file QJTable.java
///
///  Definition of the Class QJTable
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.dataengine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;

//import org.json.JSONArray;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.simba.dsi.dataengine.interfaces.IArray;
import com.simba.dsi.dataengine.utilities.ColumnMetadata;
import com.simba.dsi.dataengine.utilities.DSITypeUtilities;
import com.simba.dsi.dataengine.utilities.DataWrapper;
import com.simba.dsi.dataengine.utilities.MetadataSourceColumnTag;
import com.simba.dsi.dataengine.utilities.MetadataSourceID;
import com.simba.dsi.dataengine.utilities.Nullable;
import com.simba.dsi.dataengine.utilities.Searchable;
import com.simba.dsi.dataengine.utilities.TypeMetadata;
import com.simba.dsi.dataengine.utilities.TypeUtilities;
import com.simba.dsi.dataengine.utilities.Updatable;
import com.simba.dsi.exceptions.IncorrectTypeException;
import com.simba.dsi.exceptions.NumericOverflowException;
import com.simba.sqlengine.dsiext.dataengine.DSIExtJResultSet;
import com.simba.sqlengine.dsiext.dataengine.Identifier;
import com.simba.support.ILogger;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ErrorException;
import com.useready.rester.Settings;
import com.useready.rester.core.URCoreUtils;
import com.useready.rester.core.URDriver;
import com.useready.rester.exceptions.URMessageKey;

/**
 * Class representing a table in the QuickJson database.
 */
public class URTable extends DSIExtJResultSet {
	/*
	 * Instance variable(s)
	 * ========================================================================
	 */

	/**
	 * Connection logger.
	 */
	private final ILogger m_logger;

	/**
	 * Settings for the driver.
	 */
	private Settings m_settings;

	private String service;

	/**
	 * The table identifier to use.
	 */
	private Identifier m_tableIdentifier;

	private boolean exist = false;
	private boolean exists = false;
	/**
	 * Iterator on the root node, which represents one row of data.
	 */
	private Iterator<JsonNode> m_rootDocIterator = null;

	/**
	 * Root node, can be either an array node or an object node.
	 */
	private JsonNode m_rootNode = null;

	private JsonNode m_rootNodeList = null;

	private JsonNode m_rootNodes;

	private JsonNode m_rootCOBDate;

	private JsonNode m_coloumNode = null;

	String index = null;

	String col_index = null;
	public boolean measure = false;

	private List<String> indexList = new ArrayList<>();
	// String table=null;
	String deptName = null;
	boolean deptAtr = false;
	JsonNode dataType;

	/**
	 * Row Node, the fields of the current JSON document.
	 */
	private JsonNode m_currentRowNode = null;

	/**
	 * Initialize the row count of the current JSON document.
	 */
	private long m_rowCount = ROW_COUNT_UNKNOWN;

	/**
	 * Flag used for distinguishing if the root JSON doc is an object or array.
	 */
	private boolean m_isObject = false;

	/**
	 * Flag used for distinguishing if the JSON doc has been read or not, when the
	 * JSON doc is an object.
	 */
	private boolean m_jsonDocHasBeenRead = false;

	/**
	 * List of column metadata.
	 */
	private ArrayList<ColumnMetadata> m_metadata = new ArrayList<ColumnMetadata>();

	private Hashtable<String, ColumnMetadata> m_metadataHashtable = new Hashtable<>();

	/**
	 * List of chains, represents the list of lookups.
	 */
	private List<CellLookup> m_chain = new ArrayList<CellLookup>();

	/**
	 * List of sql types for all columns in the column metadata.
	 */
	private List<Short> m_listOfColumnTypes = new ArrayList<Short>();

	/*
	 * Constructor(s)
	 * =============================================================================
	 * =
	 */

	/**
	 * Constructor.
	 *
	 * @param logger          The logger to use.
	 * @param settings        The settings to use.
	 * @param tableIdentifier The table identifier to use.
	 *
	 * @throws ErrorException Thrown if a general error occurs.
	 * @throws IOException
	 */
	public URTable(ILogger logger, Settings settings, Identifier tableIdentifier) {
		LogUtilities.logFunctionEntrance(logger, logger, settings, tableIdentifier);

		m_logger = logger;
		m_settings = settings;
		m_tableIdentifier = tableIdentifier;
		ObjectMapper objectMapper = new ObjectMapper();
		List<JsonNode> m_rootNodeTablesList = new ArrayList<JsonNode>();
		Map<String, JsonNode> m_tablesLists = new TreeMap<String, JsonNode>();
		try {

			if (m_settings.m_tablesLists != null && m_tableIdentifier.getName() != null
					&& m_settings.m_format.equalsIgnoreCase("loadOnDemand")) {
				// LogUtilities.logDebug("-!m_settings.m_queryRun----" + !m_settings.m_queryRun,
				// logger);
				for (Map.Entry<String, List<String>> entry : m_settings.m_tablesLists.entrySet()) {

					// if(entry.getKey().equals(m_tableIdentifier.getName())) {
					if (m_settings.m_format.equalsIgnoreCase("loadOnDemand") && !m_settings.m_queryRun) {
						if (entry.getKey().equals("COB_Dates") && m_tableIdentifier.getName().equals("COB_Dates")) {
							m_rootCOBDate = m_settings.m_tablesList.get("COB_Dates");
							m_rootNodeList = m_rootCOBDate;
						} else {
							List<String> list = entry.getValue();
							String fieldName = list.get(0);
							String ColDataType = null;
							if (list.get(2) != null) {
								ColDataType = list.get(2);
							}
							String tbUrl = list.get(3);
							ObjectMapper mapper = new ObjectMapper();
							JsonNode node = mapper.readTree(list.get(4).replaceAll("\\“", "\""));
							Iterator<Entry<String, JsonNode>> firstNodeIterator = node.fields();
							int d = 0;
							int arr = 0;
							String tableName;
							while (firstNodeIterator.hasNext()) {
								Entry<String, JsonNode> object = firstNodeIterator.next();
								if (d == 1) {
									JsonNode schemaList = object.getValue();
									List<String> tbList = m_settings.displayNameComb.get(tbUrl);
									indexList = new ArrayList<>();
									for (int i = 0; i <= schemaList.size() - 1; i++) {
										JsonNode nodes = schemaList.get(i);
										if (nodes.has("colName")) {
											LogUtilities.logDebug("-m_settings.displayNameComb.size() --" + fieldName
													+ "-----" + tbList, logger);
											if (m_settings.displayNameComb != null
													&& m_settings.displayNameComb.size() > 0) {
												for (int tb = 0; tb < tbList.size(); tb++) {
													String originalString = tbList.get(tb);
													try {
														if (nodes.get("colName").toString() != null
																&& (nodes.get("colName").toString().replace("\"", ""))
																		.equals(originalString)) {
															tableName = nodes.get("idx").toString().replace("\"", "");
															tbList.set(tb, tableName);
															break;
														}
													} catch (Exception e) {
														LogUtilities.logError("-Exception e-----" + e.getMessage(),
																logger);
														e.printStackTrace();
													}
												}

											}
											if (nodes.get("colName").toString() != null
													&& (nodes.get("colName").toString().replace("\"", ""))
															.equals(fieldName)) {
												tableName = nodes.get("idx").toString().replace("\"", "");
												index = tableName;
												ObjectMapper map = new ObjectMapper();
												try {
													if (tbUrl.contains("measure")
															&& m_settings.m_measureDataType != "") {
														dataType = mapper
																.readTree("\"" + m_settings.m_measureDataType + "\"");
														measure = true;
													} else {
														LogUtilities.logDebug("-nodes.get(\"dataType\").toString()--"
																+ nodes.get("dataType").toString(), logger);
														dataType = mapper.readTree((nodes.get("dataType").toString())
																.replaceAll("\\“", "\""));
													}
												} catch (Exception e) {
													LogUtilities.logError("-Exception e-----" + e.getMessage(), logger);
													e.printStackTrace();
												}

											}
											if (nodes.get("colName").toString() != null && ColDataType != null
													&& (nodes.get("colName").toString().replace("\"", ""))
															.equals(ColDataType)) {
												measure = false;
												col_index = nodes.get("idx").toString().replace("\"", "");
												LogUtilities.logDebug("-col_index--------" + col_index, logger);

											}
										}
									}
									indexList = tbList;
									m_settings.displayNameComb.put(tbUrl, tbList);
									LogUtilities.logDebug(
											"- m_settings.displayNameComb- last-" + m_settings.displayNameComb, logger);
								}
								if (d == 2) {
									JsonNode snapShotList = object.getValue();
									m_coloumNode = snapShotList;
									try {
										if (!m_settings.iterate) {
											initializeColumns();
										} else {
											m_metadata = m_settings.m_metadataList;
											m_listOfColumnTypes = m_settings.m_listOfColumnTypesList;
										}
									} catch (ErrorException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
//									if(m_settings.str!=null) {
//									fetchData();
//									}
								}
								d++;

							}

						}

					} else {
						if (m_settings.str != null && m_settings.m_queryRun) {

							m_metadata = m_settings.m_metadataList;
							m_listOfColumnTypes = m_settings.m_listOfColumnTypesList;
							fetchData();
							m_settings.m_queryRun = false;
						}
					}
				}
				m_settings.iterate = true;
				m_settings.m_metadataList = m_metadata;
				m_settings.m_listOfColumnTypesList = m_listOfColumnTypes;
			}

			else {
				LogUtilities.logDebug("m_settings.originalColumnNames" + m_settings.originalColumnNames.size(),
						m_logger);

				LogUtilities.logDebug("m_settings.originalColumnNames" + m_settings.originalColumnNames.size(),
						m_logger);
				try {
					// Map<String, JsonNode> node = extractTableData();
					String tableName = m_tableIdentifier.getCatalog() + "/" + m_tableIdentifier.getSchema() + "/"
							+ m_tableIdentifier.getName();
					LogUtilities.logDebug("TableNAME::::::::::::::" + tableName, m_logger);

					// if (node.containsKey(tableName)) {
//						m_settings.m_rootNodeTableList = new ArrayList<JsonNode>();
//						m_settings.m_rootNodeTableList.add(node.get(tableName));
					LogUtilities.logDebug("m_rootNodeTableList::::::::::::::" + m_settings.m_rootNodeTableList,
							m_logger);
					for (int i = 0; i <= m_settings.m_fieldsArr.size() - 1; i++) {
						JsonNode nodeArr = m_settings.m_fieldsArr.get(i);
						LogUtilities.logDebug("m_settings.m_tableFields::::::::::" + nodeArr, m_logger);
						fetchTableData(nodeArr);
						LogUtilities.logDebug("m_settings.m_tableFields::::::::::" + nodeArr, m_logger);
						if (nodeArr.has("dataFields")) {
							LogUtilities.logDebug("m_settings.m_tableFields::::::::::" + nodeArr.get("dataFields"),
									m_logger);
							m_settings.m_tableFields = nodeArr.get("dataFields");
							LogUtilities.logDebug("m_settings.m_tableFields::::::::::" + m_settings.m_tableFields,
									m_logger);
						}
						// fetchTableData(nodeArr);
					}
					// }
				} catch (Exception e) {
					LogUtilities.logError("Exception--" + e.getMessage(), m_logger);
					LogUtilities.logError("Exception--" + e.getStackTrace(), m_logger);
				}

				LogUtilities.logDebug("m_settings.m_rootNodeTableList" + m_settings.m_rootNodeTableList.size(),
						m_logger);
				for (JsonNode rootNode : m_settings.m_rootNodeTableList) {
					LogUtilities.logDebug("TableList---" + rootNode, m_logger);
					m_tablesLists.put(m_tableIdentifier.getName(), rootNode);
					m_rootNodeTablesList.add(rootNode);
					m_rootNode = rootNode;
				}
				m_rootCOBDate = m_settings.m_tablesList.get("COB_Dates");
			}

		} catch (Exception e) {
		}

		// Read the entire JSON document as a root node.
		Iterator<Entry<String, JsonNode>> tableIterator = m_settings.m_rootNode.fields();
		if (m_settings.m_rootNode.size() != 0) {
			// Multiple Tables Data on onload
			int arr = 0;

			for (JsonNode node : m_rootNodeTablesList) {
				m_currentRowNode = node;
				LogUtilities.logDebug("TableList-m_currentRowNode--" + m_currentRowNode, m_logger);
				if (m_currentRowNode.isArray()) {
					LogUtilities.logDebug("TableList-m_currentRowNode is Array--" + m_currentRowNode, m_logger);
					m_coloumNode = null;
					m_coloumNode = m_currentRowNode;

					try {
						initializeColumns();
					} catch (ErrorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					JsonNode m_currentRowNode;

					// for Extra Node
					Iterator<Entry<String, JsonNode>> firstNodeIterator = node.fields();
					int d = 0;

					while (firstNodeIterator.hasNext()) {
						Entry<String, JsonNode> finalObject = firstNodeIterator.next();

						LogUtilities.logDebug("TableList-finalObject--" + finalObject.getValue(), m_logger);
						if (!(m_settings.m_levels.replace("\"", "")).equals("0")) {
							m_currentRowNode = node;

						} else {
							m_currentRowNode = finalObject.getValue();
						}
						LogUtilities.logDebug("TableList-node--" + m_currentRowNode.size(), m_logger);
						if (m_currentRowNode.size() != 0) {
							Iterator<Entry<String, JsonNode>> objectIterator = m_currentRowNode.fields();
							int i = 0;
							boolean sameCatlog = true;
							while (objectIterator.hasNext()) {
								LogUtilities.logDebug("objectIterator" + m_currentRowNode.size(), m_logger);
								Entry<String, JsonNode> object = objectIterator.next();
								sameCatlog = true;
								i++;
								LogUtilities.logDebug("objectIterator" + object.getValue().toString(), m_logger);
								if (d == 0 && i == 1 && sameCatlog && object.getValue() != null
										&& !(0 == object.getValue().size())) {
									JsonNode m_currentSchemaNode = object.getValue();

									m_coloumNode = null;
									m_coloumNode = m_currentSchemaNode;

									try {
										initializeColumns();
									} catch (ErrorException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								} else {

									if (d == 1 && m_settings.m_format.equalsIgnoreCase("loadOnDemand") && arr == 0
											&& sameCatlog && object.getValue() != null
											&& !(0 == object.getValue().size())) {
										JsonNode m_currentSchemaNode = object.getValue();
										m_rootNodeList = m_currentSchemaNode;
										sameCatlog = false;
									}

									else if (m_settings.m_format.equalsIgnoreCase("loadOnDemand") && arr == 1
											&& sameCatlog && object.getValue() != null
											&& !(0 == object.getValue().size())) {
										JsonNode m_currentSchemaNode = object.getValue();
										m_rootNodes = m_currentSchemaNode;

										sameCatlog = false;
									}

								}

							}
							if (!m_settings.m_format.equalsIgnoreCase("loadOnDemand") && d == 1 && arr == 0
									&& sameCatlog && finalObject.getValue() != null
									&& !(0 == finalObject.getValue().size())) {
								JsonNode m_currentSchemaNode = finalObject.getValue();
								m_rootNodeList = m_currentSchemaNode;
								sameCatlog = false;
							}

							else if (d == 1 && arr == 1 && sameCatlog && finalObject.getValue() != null
									&& !(0 == finalObject.getValue().size())) {
								JsonNode m_currentSchemaNode = finalObject.getValue();
								m_rootNodes = m_currentSchemaNode;

								sameCatlog = false;
							}
							d++;
						} else {
							m_rootNode = m_settings.m_rootNode;
						}

					}
				}

				arr++;
			}
			if (m_settings.m_format.equals("oneCall")) {
				for (int i = 0; i <= m_settings.m_fieldsArr.size() - 1; i++) {
//					LogUtilities.logDebug("TableList-Field Arrr--------------" + m_settings.m_fieldsArr.size(), m_logger);
//					JsonNode nodeArr = m_settings.m_fieldsArr.get(i);
//					fetchTableData(nodeArr);
				}
				if (!m_settings.iterate) {
					m_settings.iterate = true;
					m_settings.m_metadataList = m_metadata;
					m_settings.m_listOfColumnTypesList = m_listOfColumnTypes;
					m_settings.m_DataNode = m_rootNodes;
					LogUtilities.logDebug("m_settings.iterate::::::::::::::::" + m_rootNodeList, m_logger);
				}
			}
			if (m_tableIdentifier.getName() != null && !m_tableIdentifier.getName().isEmpty()) {
				if (!m_settings.m_format.equals("loadOnDemand")) {
					LogUtilities.logDebug("-m_settings.iterate" + m_settings.m_metadataList, m_logger);
					// LogUtilities.logDebug("-m_settings.iterate m_rootNodeList" + m_rootNodeList,
					// m_logger);
					if (m_settings.iterate) {
						m_metadata = m_settings.m_metadataList;
						m_listOfColumnTypes = m_settings.m_listOfColumnTypesList;
						m_rootNodes = m_settings.m_DataNode;
					}
					LogUtilities.logDebug("-m_settings.m_metadata::::::::::::::::::::" + m_rootNodes, m_logger);
					m_rootNode = m_rootNodes;
					// LogUtilities.logDebug("--m_rootNode!=null---------" + (m_rootNode != null),
					// m_logger);
					if (m_settings.m_format.equals("oneCall") && m_rootNode.size() > 0) {
						m_settings.m_actualNode = m_rootNode;
					}
					if (m_settings.m_format.equals("oneCall") && m_rootNode.size() == 0) {
						m_rootNode = m_settings.m_actualNode;
					}
					//LogUtilities.logDebug("---Modified JSON Node---------" + m_rootNode.toString() + "-", m_logger);
				} else {
					if (m_rootNodes != null) {
						// LogUtilities.logDebug("m_rootNodes::::::::::::::----" +
						// m_rootNodes.toString() + "-", m_logger);

						m_rootNode = m_rootNodes;
					//	LogUtilities.logDebug("m_rootNode::::::::::::::----" + m_rootNode.toString() + "-", m_logger);
					}

				}

				// LogUtilities.logDebug("---Modified JSON Node---------" +
				// m_rootNode.toString() + "-", m_logger);
				if (m_settings.m_format.equals("oneCall") && m_settings.m_tableFields != null) {
					LogUtilities.logDebug("- m_settings.m_tableFields-----" + m_settings.m_tableFields + "-", m_logger);
					Iterator<Entry<String, JsonNode>> firstNodeIterator = m_settings.m_tableFields.fields();
					int d = 0;

					if (m_settings.m_tableFields.isArray()) {
						for (JsonNode node : m_settings.m_tableFields) {
							String name = node.get("name").asText();
							LogUtilities.logDebug("- m_settings.m_tableFields-----" + name + "-", m_logger);
							try {
								TypeMetadata typeMeta = null;
								long columnLength = 0;
								short sqlType = 0;
								sqlType = java.sql.Types.DATE;
								typeMeta = TypeMetadata.createTypeMetadata(sqlType);
								columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
								m_coloumNode = null;
								ColumnMetadata columnMd = new ColumnMetadata(typeMeta);
								columnMd.setCaseSensitive(true);
								columnMd.setCatalogName(m_tableIdentifier.getCatalog());
								columnMd.setSchemaName(m_tableIdentifier.getSchema());
								columnMd.setTableName(m_tableIdentifier.getName());
								columnMd.setLabel(name.replace("\"", ""));
								columnMd.setName(name.replace("\"", ""));
								columnMd.setColumnLength(columnLength);
								columnMd.setNullable(Nullable.NULLABLE);
								columnMd.setSearchable(Searchable.SEARCHABLE);
								columnMd.setUpdatable(Updatable.READ_ONLY);
								m_metadata.add(columnMd);

							} catch (ErrorException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (NumericOverflowException e) {

							}
						}

					}

				}
			}
		}
	}

	private void fetchTableData(JsonNode nodeArr) {
		if (nodeArr.has("url")) {
			String newurl = null;
			if (nodeArr.get("url").toString() != null) {
				String url = nodeArr.get("url").toString();
				String queryUrl = url;
				LogUtilities.logDebug("Query url-:::::::::::::::::::" + queryUrl, m_logger);
				if (!url.isEmpty() && url != null) {
					url = m_settings.m_baseUrl.replace("\"", "") + url;
					if (url.contains("{")) {
						int index = url.indexOf("{");
						String substring = url.substring(0, index).replace("\"", "");
						newurl = substring + m_tableIdentifier.getCatalog() + "/" + m_tableIdentifier.getSchema() + "/"
								+ m_tableIdentifier.getName() + "?redirect=false";
						try {
							LogUtilities.logDebug("New Url" + newurl, m_logger);
							JsonNode rootNodess = URCoreUtils.readDatafromJson(newurl, m_logger, m_settings, null, null,
									true);
							LogUtilities.logDebug("rootNodess::::::::::::::::" + rootNodess, m_logger);
							LogUtilities.logDebug("rootNodess::::::::::::::::" + rootNodess.has("redirectUrl"),
									m_logger);
							if (rootNodess.has("redirectUrl")) {
								m_settings.m_tables = rootNodess.get("redirectUrl").toString().replace("\"", "");
								LogUtilities.logDebug("rootNodess------" + rootNodess, m_logger);
							}

						} catch (Exception e) {
							// TODO: handle exception
							LogUtilities.logError("Excep------" + e.getMessage(), m_logger);
						}
						ObjectMapper objectMapper = new ObjectMapper();
						try {

							String urlPart = null;
							LogUtilities.logDebug("Query url-:::::::::::::::::::" + queryUrl, m_logger);
							int indexOfQuestionMark = queryUrl.indexOf("?");
							int indexOfArgs = queryUrl.indexOf("args=");
							LogUtilities.logDebug("indexOfQuestionMark:::::::::::::::::::" + indexOfQuestionMark,
									m_logger);
							LogUtilities.logDebug("indexOfArgs:::::::::::::::::::" + indexOfArgs, m_logger);

							if (indexOfQuestionMark != -1 && indexOfArgs != -1) {
								// Extract the data between "?" and "args="
								urlPart = queryUrl.substring(indexOfQuestionMark, indexOfArgs + 5);

							}
							LogUtilities.logDebug("urlPart:::::::::::::::::::" + urlPart, m_logger);
							String st = null;
							for (String s : m_settings.originalColumnNames) {
								if (st == null) {
									st = "\"" + s + "\"";
								} else {
									st = st + "," + "\"" + s + "\"";
								}
							}
							st = "[" + st + "]";
//        						    	 
//        						    	  
							LogUtilities.logDebug("Updated URL)---" + st, m_logger);
							// Update the "kwargs" dynamically
							// ((ObjectNode) apiData).put("sub_region_1", "California");
							String filter = "{\"columns\":" + st + ",\"kwargs\":{";
							if (m_settings.m_filter != null) {
								filter = filter + m_settings.m_filter + "}}";
							} else {
								filter = filter + "}}";
							}
							String updatedApiURLString = m_settings.m_tables + urlPart + URLEncoder.encode(filter);

							// queryUrl.substring(0, queryUrl.indexOf('?') + 1) +
							// apiData.toString();
							// forTesting
							m_settings.m_tables = updatedApiURLString;

							LogUtilities.logDebug("Updated URL)---" + m_settings.m_tables, m_logger);
						} catch (Exception e) {
							LogUtilities.logError("Exception-------" + e.getMessage(), m_logger);
							e.getStackTrace();
						}

						String dbpath = m_settings.m_tables;
						JsonNode rootNodess = URCoreUtils.readDatafromJson(dbpath, m_logger, m_settings, null, null,
								false);
						LogUtilities.logDebug("Nodes Object-" + rootNodess, m_logger);
						// m_settings.m_TablesData.put(url, rootNodess);
						m_settings.m_tableData = true;
						Iterator<Entry<String, JsonNode>> firstDataIterator = rootNodess.fields();
						int df = 0;

						while (firstDataIterator.hasNext()) {
							Entry<String, JsonNode> finalObject = firstDataIterator.next();

							JsonNode currentRowNode = finalObject.getValue();
							if (df == 0) {
								if (m_settings.m_format.equalsIgnoreCase("oneCall") && finalObject.getValue() != null
										&& !(0 == finalObject.getValue().size())) {
									JsonNode m_currentSchemaNode = finalObject.getValue();
									// m_rootNodes = m_currentSchemaNode;
									// LogUtilities.logDebug("m_rootNodes Object-" + m_rootNodes, m_logger);
									m_settings.m_rootNodeTableList = new ArrayList<JsonNode>();
									m_settings.m_rootNodeTableList.add(m_currentSchemaNode);
									LogUtilities.logDebug("m_settings.m_rootNodeTableList---------------"
											+ m_settings.m_rootNodeTableList, m_logger);

								}
							}
							if (df == 1) {
								if (df == 1 && m_settings.m_format.equalsIgnoreCase("oneCall")
										&& finalObject.getValue() != null && !(0 == finalObject.getValue().size())) {
									JsonNode m_currentSchemaNode = finalObject.getValue();
									m_rootNodes = m_currentSchemaNode;
									LogUtilities.logDebug("m_rootNodes Object-" + m_rootNodes, m_logger);
								}
							}
							df++;

						}
					}
				}
			}
		}

	}

	/*
	 * Method(s)
	 * =============================================================================
	 * ======
	 */

	/**
	 * Closes the ResultSet and releases any resources held by it.
	 *
	 */
	@Override
	public void close() {
		LogUtilities.logFunctionEntrance(m_logger);
	}

	/**
	 * Get the catalog name of the table.
	 *
	 * @return the catalog name.
	 */
	@Override
	public String getCatalogName() {
		LogUtilities.logFunctionEntrance(m_logger);
		return m_tableIdentifier.getCatalog();
	}

	/**
	 * Fills in retrievedData with the corresponding chunk of data. retrievedData is
	 * an uninitialized DataWrapper, and must be modified by this call.
	 * <p>
	 * Data is requested from column for the row that the internal cursor is
	 * currently positioned at. column is given as a 0-based index, matching the
	 * index of the corresponding column metadata in the ArrayList returned by
	 * getSelectColumns().
	 * <p>
	 * For character and binary data types, offset is used to indicate an offset
	 * within the data cell from which to begin the retrieved chunk. maxSize is used
	 * to indicate the suggested maximum size of the retrieved data chunk. offset
	 * and maxSize are both given in bytes, and should have no effect when
	 * retrieving fixed-width data. The value RETRIEVE_ALL_DATA may be provided as
	 * maxSize to indicate that all available data is to be retrieved.
	 * <p>
	 * The return value indicates if there is more data left in the current cell.
	 * The return value should always be false when retrieving data of a fixed-width
	 * type.
	 * <p>
	 * If the driver attribute DSI_RETRIEVE_DATA_ORDER_RESTRICTION is set to
	 * DSI_RETRIEVE_DATA_IN_NONDECREASING_ORDER, data will only be requested from a
	 * row in non-decreasing column order. Data within a single cell will always be
	 * requested in increasing offset order, with no chunk overlap.
	 * <p>
	 * When requesting chunks of character and binary data, maxSize will typically
	 * match the chunk size specified in the DSI_MAXIMUM_RETRIEVE_DATA_CHUNK_SIZE
	 * driver attribute when it isn't set to RETRIEVE_ALL_DATA.
	 * <p>
	 * This method will not be called until IQueryExecutor.execute() has been called
	 * on the corresponding IQueryExecutor.
	 *
	 * @param column        Column for which the data is requested for the row that
	 *                      the internal cursor is currently positioned at. Is a
	 *                      0-based index, matching the index of the corresponding
	 *                      column metadata in the ArrayList returned by
	 *                      GetSelectColumns().
	 * @param offset        Used to indicate an offset within the data cell from
	 *                      which to begin the retrieved chunk for character and
	 *                      binary data types. Value given is in bytes and should
	 *                      have no effect when retrieving fixed-width data.
	 * @param maxSize       The suggested maximum size of the retrieved data chunk.
	 *                      Value given is in bytes and should have no effect when
	 *                      retrieving fixed-width data.
	 * @param retrievedData An uninitialized DataWrapper, and must be modified by
	 *                      this call. Is expected to be filled in with the
	 *                      corresponding chunk of data.
	 *
	 * @return <code>true</code> if more data is left in the current cell;
	 *         <code>false</code> otherwise. Return value should always be
	 *         <code>false</code> when retrieving data of a fixed-width type.
	 *
	 * @throws ErrorException Thrown if a general error occurs.
	 *
	 * @see DataWrapper
	 */
	public boolean getData(int column, long offset, long maxSize, DataWrapper retrievedData) throws ErrorException {
		try {
			JsonNode node = null;
			String datatype = "STRING";
			LogUtilities.logDebug("---------Retrived Data node--" + column, m_logger);
			LogUtilities.logDebug("m_settings.m_columnIndexData:::::::::::::::::::;" + m_settings.m_columnIndexData,
					m_logger);
			if (m_currentRowNode.isArray()) {
				if (m_settings.m_format.equalsIgnoreCase("oneCall")) {
					node = m_currentRowNode.get(column - 1);
				} else {
					ColumnMetadata metaData = m_metadata.get(column);
					LogUtilities.logDebug("-Meta Data Name :::::::::::::-" + metaData.getName() + " Datatype::;"
							+ metaData.getTypeMetadata().getTypeName(), m_logger);
					datatype = metaData.getTypeMetadata().getTypeName().toString();
					String s = null;
					String str = metaData.getName();
					int col = m_settings.m_columnIndexData.get(str);
					LogUtilities.logDebug("m_columnIndexData index:::::::::-" + col, m_logger);
					node = m_currentRowNode.get(col);
					LogUtilities.logDebug("m_columnIndexData Node:::::-" + node, m_logger);
				}

//				if (m_rootNode.isObject()) {
//					node = m_currentRowNode.get(column);
//				}

			} else {

				if (m_settings.m_format.equalsIgnoreCase("oneCall")) {
					ColumnMetadata metaData = m_metadata.get(column);
					LogUtilities.logDebug("-Meta Data Name :::::::::::::-" + metaData.getName(), m_logger);
					node = m_currentRowNode.get(metaData.getName());
					LogUtilities.logDebug("---------Retrived Data Data--" + node, m_logger);
					datatype = metaData.getTypeMetadata().getTypeName().toString();
					LogUtilities.logDebug("---------Retrived Data nodeType--" + datatype, m_logger);
				}
			}
			// Detect null value early.
			if ((null == node) || // Missing node.
					node.isNull()) // || // Literal null value.
			{
				retrievedData.setNull(m_listOfColumnTypes.get(column));
				return false;
			}
			switch (datatype) {
			case "NUMBER": {
				try {
					// Set "NUMERIC" as the SQL type if it's a valid number in
					// com.fasterxml.jackson.core.json.UTF8StreamJsonParser.
					// i.e. integers > 9223372036854775807 or < -9223372036854775808
					retrievedData.setNumeric(new BigDecimal(node.asText().replace("\"", "")));
					return false;
				} catch (Exception e) {
					LogUtilities.logError(e.getMessage(), m_logger);
				}
			}
			case "BIGINT": {
				try {
					retrievedData.setBigInt(new BigInteger((node.asText().replace("\"", ""))));
					return false;
				} catch (Exception e) {
					LogUtilities.logError(e.getMessage(), m_logger);
				}
			}
			case "FLOAT": {
				try {
					retrievedData.setDouble(new Double(node.asText().replace("\"", "")));
					return false;

				} catch (Exception e) {
					LogUtilities.logError(e.getMessage(), m_logger);
				}
			}
			case "DOUBLE": {
				try {
					// All decimal numbers set to be "DOUBLE"
					retrievedData.setDouble(new Double(node.asText().replace("\"", "")));
					return false;
				} catch (Exception e) {
					LogUtilities.logError(e.getMessage(), m_logger);
				}
			}
			case "NUMERIC": {
				try {
					// Retrieve data as numeric.
					// retrievedData.setNumeric(new BigDecimal(node.asText().replace("\"", "")));
					if (node.isInt() || node.isLong() || node.isBigInteger()) {
						// Integers between -9223372036854775808 and 9223372036854775807 have been
						// set to SQL type "BIGINT".
						retrievedData.setBigInt(new BigInteger((node.asText().replace("\"", ""))));
						return false;
					} else if (node.isDouble() || node.isFloat()) {
						// All decimal numbers set to be "DOUBLE"
						retrievedData.setDouble(new Double(node.asText().replace("\"", "")));
						return false;
					} else {
						// Set "NUMERIC" as the SQL type if it's a valid number in
						// com.fasterxml.jackson.core.json.UTF8StreamJsonParser.
						// i.e. integers > 9223372036854775807 or < -9223372036854775808
						retrievedData.setNumeric(new BigDecimal(node.asText().replace("\"", "")));
						return false;
					}

				} catch (Exception e) {
					LogUtilities.logError(e.getMessage(), m_logger);
				}
			}

			case "STRING":
			case "VARCHAR": {
				try {
					LogUtilities.logDebug("String::::::::" + node.toString(), m_logger);
					return DSITypeUtilities.outputString(node.textValue(), retrievedData, offset, maxSize,
							java.sql.Types.VARCHAR);
				} catch (IncorrectTypeException e) {
					ErrorException ee = URDriver.s_QJMessages
							.createGeneralException(URMessageKey.DATA_TYPE_MISMATCH.name());
					LogUtilities.logError(ee, m_logger);
					throw ee;
				}
			}
			case "ARRAY": {
				try {
					return DSITypeUtilities.outputString(node.toString(), retrievedData, offset, maxSize,
							m_listOfColumnTypes.get(column));
				} catch (IncorrectTypeException e) {
					ErrorException ee = URDriver.s_QJMessages
							.createGeneralException(URMessageKey.DATA_TYPE_MISMATCH.name());
					LogUtilities.logError(ee, m_logger);
					throw ee;
				}

			}
			case "BIT": {
				// Retrieve data as bit.
				try {
					retrievedData.setBoolean(node.asBoolean());
					return false;
				} catch (Exception e) {
					LogUtilities.logDebug("Exception::::::::::::::" + e.getMessage(), m_logger);
					// LogUtilities.logError(e.getMessage(), m_logger);
				}
			}
			case "CLOB": {
				// Retrieve data as bit.
				LogUtilities.logDebug("-Node value::::::::::::::::" + node.asBoolean() + "-------", m_logger);
				try {
					retrievedData.setClob((Clob) node);
					return false;
				} catch (Exception e) {
					LogUtilities.logDebug("Exception::::::::::::::" + e.getMessage(), m_logger);
					// LogUtilities.logError(e.getMessage(), m_logger);
				}
			}
			case "DATE":
			case "TYPE_DATE": {
				// Retrieve data as Date.
				try {

					ObjectMapper objectMapper = new ObjectMapper();
					Date date = objectMapper.readValue(node.toString(), Date.class);
					LogUtilities.logDebug("DateTime:::::::" + date, m_logger);
					retrievedData.setDate(new java.sql.Date(date.getTime()));
					LogUtilities.logDebug("Date:::::::" + new java.sql.Date(date.getTime()), m_logger);
					return false;
					// }
				} catch (Exception e) {
					LogUtilities.logError("Exception:  Datetime:::::::::::::" + e.getMessage(), m_logger);
					// LogUtilities.logError(e.getMessage(), m_logger);
				}
			}
			case "DATETIME": {
				// Retrieve data as Datetime.
				try {
					ObjectMapper objectMapper = new ObjectMapper();
					Date date = objectMapper.readValue(node.toString(), Date.class);
					LogUtilities.logDebug("DateTime:::::::" + date, m_logger);
					retrievedData.setDate((java.sql.Date) date);
					LogUtilities.logDebug("DateTime:::::::" + new java.sql.Date(date.getTime()), m_logger);
					return false;
					// }
				} catch (Exception e) {
					LogUtilities.logError("Exception::::::::::::::" + e.getMessage(), m_logger);
					// LogUtilities.logError(e.getMessage(), m_logger);
				}
			}
			case "BOOLEAN": {
				// Retrieve data as bit.
				LogUtilities.logDebug("-Node value::::::::::::::::" + node.asBoolean() + "-------", m_logger);
				try {
					retrievedData.setBoolean(node.asBoolean());
					return false;
				} catch (Exception e) {
					LogUtilities.logError("Exception::::::::::::::" + e.getMessage(), m_logger);
					// LogUtilities.logError(e.getMessage(), m_logger);
				}
			}
			case "NULL": {
				LogUtilities.logDebug("-Node value::::::::::::::::" + node.asBoolean() + "-------", m_logger);
				retrievedData.setNull(m_listOfColumnTypes.get(column));
				return false;
			}

			default: {
				throw URDriver.s_QJMessages.createGeneralException(URMessageKey.UNKNOWN_JSON_TYPE.name(),
						new String[] { node.getNodeType().toString() });
			}
			}
		} catch (Exception e) {
			LogUtilities.logDebug("Exception :::" + e.getMessage(), m_logger);
			return false;
		}
	}

	/**
	 * Returns the number of rows in the result set. The value returned is undefined
	 * if the row count is unknown, in which case hasRowCount() should return false.
	 * <p>
	 * The value returned must fit into a 32-bit integer when targeting a 32-bit
	 * platform, or a 64-bit integer for a 64-bit platform. The value must be non-
	 * negative, except if the row count is unknown.
	 * <p>
	 * This method will not be called until IQueryExecutor.execute() has been called
	 * on the corresponding IQueryExecutor.
	 *
	 * @return The number of rows in the JSON doc.
	 */
	public long getRowCount() {
		LogUtilities.logFunctionEntrance(m_logger);
		return m_rowCount;
	}

	/**
	 * Get the schema name of the table.
	 *
	 * @return the schema name.
	 */
	@Override
	public String getSchemaName() {
		LogUtilities.logFunctionEntrance(m_logger);
		return m_tableIdentifier.getSchema();
	}

	/**
	 * Returns an ArrayList of ColumnMetadatas, corresponding to the columns for
	 * which data is provided in the result set. Even if there are no rows in the
	 * result set, the column metadata should still be accurate. Position in the
	 * ArrayList should match position in the result set. The first column should be
	 * found at position 0, the second at 1, and so on.
	 *
	 * @return The column metadata arraylist.
	 */
	public ArrayList<ColumnMetadata> getSelectColumns() {
		LogUtilities.logFunctionEntrance(m_logger, m_metadata);
		return m_metadata;
	}

	/**
	 * Get the name of the table.
	 *
	 * @return the table name.
	 */
	@Override
	public String getTableName() {
		LogUtilities.logFunctionEntrance(m_logger);

		// LogUtilities.logDebug("------------------------------------Table Name
		// ---",m_logger);
		return m_tableIdentifier.getName();
	}

	/**
	 * Returns true if the ResultSet has more rows to fetch. Returns false if there
	 * are no more rows to fetch.
	 *
	 * @return true if there are more rows to fetch, false otherwise.
	 */
	public boolean hasMoreRows() throws ErrorException {
		LogUtilities.logFunctionEntrance(m_logger);

		if (m_isObject) {
			return false;
		}

		return (m_rootDocIterator.hasNext());
	}

	/**
	 * Returns true if the ResultSet has a known row count. Returns false if the
	 * number of rows in the result set is unknown.
	 *
	 * @return true if there is row count; false, if there isn't.
	 */
	public boolean hasRowCount() {
		LogUtilities.logFunctionEntrance(m_logger);

		if (ROW_COUNT_UNKNOWN == m_rowCount) {
			return false;
		}

		return true;
	}

	/**
	 * Resets the result set's to the first JSON object.
	 *
	 * @throws ErrorException If given an error
	 */
	@Override
	public void reset() throws ErrorException {
		LogUtilities.logFunctionEntrance(m_logger);

		if (m_isObject) {
			m_jsonDocHasBeenRead = false;
		} else {
			JsonNode json = null;
			LogUtilities.logDebug("--reset-----------------" + m_rootNode + "-", m_logger);
			m_rootDocIterator = m_rootNode.elements();

			LogUtilities.logDebug("-Roc DocIterartor----------" + m_rootNode.toString() + "-", m_logger);
		}
	}

	@Override
	protected void doCloseCursor() throws ErrorException {
		LogUtilities.logFunctionEntrance(m_logger);
		reset();
	}

	/**
	 * Move to the next row and load one row of data if there exists
	 *
	 * @throws ErrorException If given an error
	 *
	 * @return true, if there is the next row; false, if there isn't
	 */
	@Override
	protected boolean doMoveToNextRow() throws ErrorException {
		if (m_isObject) {
			if (!m_jsonDocHasBeenRead) {
				// The JSON doc has not been read.
				m_jsonDocHasBeenRead = true;
				return true;
			}

			return false;
		}

		if (m_rootDocIterator.hasNext()) {
			// Each iterator points to a row of data.
			m_currentRowNode = m_rootDocIterator.next();
			LogUtilities.logDebug("--DoMove to Next to Row------------" + m_currentRowNode + "-", m_logger);
//            if (!m_currentRowNode.isArray()) {
//                // The current row node in the root array node is not a JSON object.
//                ErrorException ee = URDriver.s_QJMessages.createGeneralException(
//                        URMessageKey.UNSUPPORTED_JSON_FORMAT.name(), new String[] {
//                                "the current row node in the root array node is not a JSON " + "Array"
//                        });
//                LogUtilities.logError(ee, m_logger);
//                throw ee;
//            }

			return true;
		}

		return false;
	}

	/**
	 * Add column metadata by using the JSON doc or the first object of the JSON
	 * doc.
	 *
	 * @param columnName  The column name of the field.
	 * @param columnValue The column value of the field.
	 *
	 * @throws Exception Thrown if a general error occurs.
	 */
	private void addColumnMetadata(String columnName, JsonNode columnValue) throws ErrorException {
		TypeMetadata typeMeta = null;
		long columnLength = 0;
		columnName = columnName.replace("\"", "");
		try {
			// do not add Duplicate columns.
			if (m_metadataHashtable.containsKey(columnName.toUpperCase())) {

				LogUtilities.logDebug("Duplicate Column Name not adding in list------" + columnName, m_logger);
				return;
			}

			short sqlType = 0;
			LogUtilities.logDebug(
					"--------addColumnMetadata Array--------------------------"
							+ (columnValue.toString().replace("\"", "")).toUpperCase() + "-------" + columnName,
					m_logger);
			String str = null;
//			if (m_settings.m_authType.equals("oauth")) {
//				str = "STRING";
//			} else {
			str = (columnValue.toString().replace("\"", "")).toUpperCase();
			// }
			LogUtilities.logDebug("Str Datatype:::::::" + str, m_logger);
			switch (str) {
			case "NUMBER": {
				if (columnValue.isInt() || columnValue.isLong()) {
					// Integers between -9223372036854775808 and 9223372036854775807 have been
					// set to SQL type "BIGINT".
					sqlType = java.sql.Types.BIGINT;
				} else if (columnValue.isDouble()) {
					// All decimal numbers set to be "DOUBLE"
					sqlType = java.sql.Types.DOUBLE;
				} else {
					// Set "NUMERIC" as the SQL type if it's a valid number in
					// com.fasterxml.jackson.core.json.UTF8StreamJsonParser.
					// i.e. integers > 9223372036854775807 or < -9223372036854775808
					sqlType = java.sql.Types.NUMERIC;
				}
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}
			case "BIGINT": {
				sqlType = java.sql.Types.BIGINT;
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}
			case "DOUBLE": {
				LogUtilities.logDebug("---Double------" + (columnValue.toString().replace("\"", "")).toUpperCase(),
						m_logger);
				// All decimal numbers set to be "DOUBLE"
				sqlType = java.sql.Types.DOUBLE;

				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}
			case "DECIMAL": {
				LogUtilities.logDebug("---Double------" + (columnValue.toString().replace("\"", "")).toUpperCase(),
						m_logger);
				// All decimal numbers set to be "DOUBLE"
				sqlType = java.sql.Types.DOUBLE;

				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}
			case "FLOAT": {
				LogUtilities.logDebug("---Double------" + (columnValue.toString().replace("\"", "")).toUpperCase(),
						m_logger);
				// All decimal numbers set to be "Float"
				sqlType = java.sql.Types.FLOAT;

				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}
			case "INT64": {

				// Set "NUMERIC" as the SQL type if it's a valid number in
				// com.fasterxml.jackson.core.json.UTF8StreamJsonParser.
				// i.e. integers > 9223372036854775807 or < -9223372036854775808
				sqlType = java.sql.Types.BIGINT;

				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}
			case "INT32": {

				// Set "NUMERIC" as the SQL type if it's a valid number in
				// com.fasterxml.jackson.core.json.UTF8StreamJsonParser.
				// i.e. integers > 9223372036854775807 or < -9223372036854775808
				sqlType = java.sql.Types.BIGINT;

				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}

			case "INTEGER": {

				// Set "NUMERIC" as the SQL type if it's a valid number in
				// com.fasterxml.jackson.core.json.UTF8StreamJsonParser.
				// i.e. integers > 9223372036854775807 or < -9223372036854775808
				sqlType = java.sql.Types.BIGINT;

				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}
			case "STRING": {
//				sqlType = java.sql.Types.VARCHAR;
//				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
//				columnLength = m_settings.m_maxColumnSize;
//				break;
				String columnValueAsText = columnValue.textValue();

				if (columnValueAsText.length() > m_settings.m_maxColumnSize) {
					// Read the length of the column value as a string.
					StringBuilder valueLength = new StringBuilder();
					valueLength.append(columnValueAsText.length());
					String valueLengthToString = valueLength.toString();

					// Read the maximal column length as a string.
					StringBuilder maxColumnLength = new StringBuilder();
					maxColumnLength.append(m_settings.m_maxColumnSize);
					String maxColumnLengthToString = maxColumnLength.toString();

					// If the value length is larger than the max column size, throw an
					// error exception.
					ErrorException ee = URDriver.s_QJMessages.createGeneralException(
							URMessageKey.MAX_COLUMN_SIZE_VIOLATION.name(),
							new String[] { valueLengthToString, maxColumnLengthToString });
					LogUtilities.logError(ee, m_logger);
					throw ee;
				}

				// Intentional fall-through.
			}
			case "PYOBJ": {
				String columnValueAsText = columnValue.textValue();

				if (columnValueAsText.length() > m_settings.m_maxColumnSize) {
					// Read the length of the column value as a string.
					StringBuilder valueLength = new StringBuilder();
					valueLength.append(columnValueAsText.length());
					String valueLengthToString = valueLength.toString();

					// Read the maximal column length as a string.
					StringBuilder maxColumnLength = new StringBuilder();
					maxColumnLength.append(m_settings.m_maxColumnSize);
					String maxColumnLengthToString = maxColumnLength.toString();

					// If the value length is larger than the max column size, throw an
					// error exception.
					ErrorException ee = URDriver.s_QJMessages.createGeneralException(
							URMessageKey.MAX_COLUMN_SIZE_VIOLATION.name(),
							new String[] { valueLengthToString, maxColumnLengthToString });
					LogUtilities.logError(ee, m_logger);
					throw ee;
				}

//                    LogUtilities.logDebug("--------DataType Array--------------------------"+ sqlType+"-------", m_logger);
//                    
//                    sqlType = java.sql.Types.ARRAY;
//                    typeMeta = TypeMetadata.createTypeMetadata(sqlType);
//                    columnLength = m_settings.m_maxColumnSize;
//                    break;

				// Intentional fall-through.
			}
			case "NULL": {
				sqlType = java.sql.Types.VARCHAR;
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = m_settings.m_maxColumnSize;
				break;
			}
			case "CLOB": {
				sqlType = java.sql.Types.CLOB;
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = m_settings.m_maxColumnSize;
				break;
			}

			case "BOOLEAN": {
				sqlType = java.sql.Types.BOOLEAN;
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				LogUtilities.logDebug("Boolean DataType-" + sqlType, m_logger);
				break;
			}
			case "BIT": {

				sqlType = java.sql.Types.BOOLEAN;
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				LogUtilities.logDebug("BIT DataType-" + sqlType, m_logger);
				break;

//				sqlType = java.sql.Types.VARCHAR;
//				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
//				columnLength = m_settings.m_maxColumnSize;
				// break;
			}

			case "DATETIME": {
				sqlType = java.sql.Types.DATE;
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}
			case "DATE": {
				sqlType = java.sql.Types.DATE;
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}

			case "TIME": {
				sqlType = java.sql.Types.TIME;
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = TypeUtilities.getColumnSize(typeMeta, typeMeta.getPrecision());
				break;
			}
			case "STRING[]": {
				sqlType = java.sql.Types.ARRAY;
				typeMeta = TypeMetadata.createTypeMetadata(sqlType);
				columnLength = m_settings.m_maxColumnSize;
				break;

				// Intentional fall-through.
			}
//			default: {
//				throw URDriver.s_QJMessages.createGeneralException(URMessageKey.UNKNOWN_JSON_TYPE.name(),
//						new String[] { columnValue.getNodeType().toString() });
//			}
			}// ~ switch ((columnValue.toString().replace("\"", "")).toUpperCase()) {
				// Add the column metadata of the current column to the list.
			ColumnMetadata columnMd = new ColumnMetadata(typeMeta);
			columnMd.setCaseSensitive(true);
			columnMd.setCatalogName(m_tableIdentifier.getCatalog());
			columnMd.setSchemaName(m_tableIdentifier.getSchema());
			columnMd.setTableName(m_tableIdentifier.getName());
			columnMd.setLabel(columnName);
			columnMd.setName(columnName);
			columnMd.setColumnLength(columnLength);
			columnMd.setNullable(Nullable.NULLABLE);
			columnMd.setSearchable(Searchable.SEARCHABLE);
			columnMd.setUpdatable(Updatable.READ_ONLY);
			LogUtilities.logDebug("Column Name-----" + columnMd.getName(), m_logger);
			LogUtilities.logDebug("Column Name-- sql Type---" + sqlType, m_logger);
			m_listOfColumnTypes.add(sqlType);
			LogUtilities.logDebug("Adding unique Column Name-------" + columnMd.getName(), m_logger);
			m_metadataHashtable.put(columnMd.getName().toUpperCase(), columnMd);
			if (m_settings.m_format.equals("oneCall")) {
				m_settings.originalColumnNames.add(columnName);
			}

		} catch (ErrorException err) {
			LogUtilities.logError(err, m_logger);
			throw err;
		} catch (NumericOverflowException e) {
			ErrorException ee = URDriver.s_QJMessages.createGeneralException(URMessageKey.NUMERIC_OVERFLOW.name(), e);
			LogUtilities.logError(ee, m_logger);
			throw ee;
		}
	}

	/**
	 * Flatten nested arrays.
	 *
	 * @param baseName The column name of the current object being flattened.
	 * @param node     The value field of the current object being flattened.
	 * @param chain    The current lookup chain.
	 *
	 * @throws Exception Thrown if a general error occurs.
	 */
	private void flattenArray(String baseName, JsonNode node, CellLookup chain) throws ErrorException {
		if (0 == node.size()) {
			return;
		}

		for (int i = 0; i < node.size(); ++i) {
			// Appends array index to the parent column name.
			StringBuilder name = new StringBuilder();
			name.append(baseName).append("_ArrayIndex").append(i);

			// Make a copy of chain; chain can be reused for building up the lookup for next
			// column.
			CellLookup localChain = chain.clone();

			// Add a new cell index lookup object as a child of the previous cell look up
			// object.
			localChain.addChild(new CellIndexLookup(i));

			if (node.get(i).isObject()) {
				// Pass in the chain to new levels of recursion to build it up.
				flattenObject(name.toString(), node.get(i), localChain);
				LogUtilities.logDebug("--------flattenObject 1-------------------------", m_logger);
			} else if (node.get(i).isArray()) {
				// Pass in the chain to new levels of recursion to build it up.
				flattenArray(name.toString(), node.get(i), localChain);
			} else {
				addColumnMetadata(name.toString(), node.get(i));
				LogUtilities.logDebug("--------addColumnMetadata Array- 1-------------------------", m_logger);
				m_chain.add(localChain);
			}
		}
	}

	/**
	 * Flatten nested objects.
	 *
	 * @param baseName The column name of the current object being flattened.
	 * @param node     The value field of the current object being flattened.
	 * @param chain    The current lookup chain.
	 *
	 * @throws ErrorException Thrown if a general error occurs.
	 */
	private void flattenObject(String baseName, JsonNode node, CellLookup chain) throws ErrorException {
		Iterator<Entry<String, JsonNode>> objectIterator = node.fields();

		if (!objectIterator.hasNext()) {
			return;
		}
		int i = 0;
		StringBuilder name = new StringBuilder();
		JsonNode value = null;
		while (objectIterator.hasNext()) {
			Entry<String, JsonNode> object = objectIterator.next();

			if (i == 0) {

				if (object.getKey().isEmpty()) {
					// Throw error exception if the key name is empty: "".
					ErrorException ee = URDriver.s_QJMessages
							.createGeneralException(URMessageKey.INVALID_BLANK_COLUMN_NAME.name());
					LogUtilities.logError(ee, m_logger);
					throw ee;
				}

				// Appends attribute name to the parent column name.

				if (null == baseName) {
					// Append the current key name directly when the baseName is null.
					// This happens when the node passed in is a row node.
					name.append(object.getValue());
				} else {
					name.append(baseName).append("_").append(object.getKey());
				}

				CellLookup localChain;

				if (null == chain) {
					// Create a new lookup chain when the chain is null.
					// This happens when the node passed in is a row node.
					localChain = new CellDictionaryLookup(object.getValue().toString().replace("\"", ""));
				} else {
					// Make a copy of chain; chain can be reused for building up the lookup for the
					// next
					// column.
					localChain = chain.clone();

					// Add a new cell dictionary lookup object as the child of the previous cell
					// lookup
					// object.
					localChain.addChild(new CellDictionaryLookup(object.getValue().toString().replace("\"", "")));
				}

				m_chain.add(localChain);
//                }	
			} else if (i == 1) {
				value = object.getValue();
				addColumnMetadata(name.toString(), value);
				LogUtilities.logDebug("--------addColumnMetadata Array- 2-------------------------", m_logger);
				// CellLookup localChain;

			}

			i++;
		}
	}

	private void flattenObjects(String baseName, JsonNode node, CellLookup chain) throws ErrorException {
		Iterator<Entry<String, JsonNode>> objectIterator = node.fields();

		if (!objectIterator.hasNext()) {
			if (node.isArray()) {
				// for (int i = 0; i <= node.size() - 1; i++) {
//                    JsonNode no=node.get(i);
				JsonNode no = node;
				String str = null;
				if (indexList != null && indexList.size() > 0) {
					// String newstr=null;
					for (String srt : indexList) {
						if (str == null) {
							str = no.get(Integer.parseInt(srt)).toString();
						} else {
							str = str + "." + no.get(Integer.parseInt(srt)).toString();
						}
					}

				}
				if (str == null) {
					str = no.get(Integer.parseInt(index)).toString();
				} else {
					str = str + "." + no.get(Integer.parseInt(index)).toString();
				}
				LogUtilities.logDebug("Str:::::::::::::::--" + str, m_logger);
				if (measure) {
					addColumnMetadata(str, dataType);
				} else {
					String dType = null;
					if (col_index != null) {
						dType = no.get(Integer.parseInt(col_index)).toString();
					} else {
						dType = "\"" + "string" + "\"";
					}
					ObjectMapper map = new ObjectMapper();
					JsonNode n = null;
					try {
						n = map.readTree(dType.replaceAll("\\“", "\""));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					try {
						// LogUtilities.logDebug("JsonNode::::::::::::-----" + n, m_logger);
						addColumnMetadata(str, n);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				// }

				/// }
			} else {
				return;
			}
		} // end object iterator
		int i = 0;
		StringBuilder name = new StringBuilder();
		JsonNode value = null;
		while (objectIterator.hasNext()) {
			Entry<String, JsonNode> object = objectIterator.next();

			if (i == 1) {

				if (object.getKey().isEmpty()) {
					// Throw error exception if the key name is empty: "".
					ErrorException ee = URDriver.s_QJMessages
							.createGeneralException(URMessageKey.INVALID_BLANK_COLUMN_NAME.name());
					LogUtilities.logError(ee, m_logger);
					throw ee;
				}

				// Appends attribute name to the parent column name.

				if (null == baseName) {
					// Append the current key name directly when the baseName is null.
					// This happens when the node passed in is a row node.
					name.append(object.getValue());
				} else {
					name.append(baseName).append("_").append(object.getKey());
				}

				CellLookup localChain;

				if (null == chain) {
					// Create a new lookup chain when the chain is null.
					// This happens when the node passed in is a row node.
					localChain = new CellDictionaryLookup(object.getValue().toString());
				} else {
					// Make a copy of chain; chain can be reused for building up the lookup for the
					// next
					// column.
					localChain = chain.clone();

					// Add a new cell dictionary lookup object as the child of the previous cell
					// lookup
					// object.
					localChain.addChild(new CellDictionaryLookup(object.getValue().toString()));
				}

			} else if (i == 2) {
				value = object.getValue();
				addColumnMetadata(name.toString(), value);
				LogUtilities.logDebug("--------addColumnMetadata Array- 4-------------------------", m_logger);
			}

			i++;
		}
	}

	/**
	 * Initialize columns from the JSON document.
	 *
	 * @throws ErrorException Thrown if a general error occurs.
	 */
	private void initializeColumns() throws ErrorException {
		// LogUtilities.logDebug("--initializeColumns--inside----------" +
		// m_coloumNode.toString(), m_logger);

		if (!m_coloumNode.isContainerNode()) {

			// Throw exception if the root node is not array or object.
			ErrorException ee = URDriver.s_QJMessages.createGeneralException(URMessageKey.FILE_FORMAT_ERROR.name());
			LogUtilities.logError(ee, m_logger);
			throw ee;
		}

		if (m_coloumNode.isObject()) {
			// The JSON document is an object.
			m_currentRowNode = m_coloumNode;

			// Set m_isObject to be true since the root node is an object.
			m_isObject = true;

			// Set the size of the JSON document.
			m_rowCount = 1;
			// flattenObjects(null, m_currentRowNode, null);
		} else {
			// The JSON document is an array.
			m_rootDocIterator = m_coloumNode.elements();

			if (0 == m_coloumNode.size()) {
				// Throw error exception if the root array is empty.
				ErrorException ee = URDriver.s_QJMessages.createGeneralException(URMessageKey.EMPTY_ARRAY.name());
				LogUtilities.logError(ee, m_logger);
				throw ee;
			}

			// Get the first element in the root array.
//            if (m_settings.m_levels.equals("0")) {
			for (int i = 0; i <= m_coloumNode.size() - 1; i++) {
				m_currentRowNode = m_coloumNode.get(i);

//				LogUtilities.logDebug("--initializeColumns--inside-for---------" + m_currentRowNode.toString(),
//						m_logger);
				if (m_settings.m_levels.equals("0")) {
					// m_currentRowNode = m_coloumNode;
					flattenObjects(null, m_currentRowNode, null);
				} else {
					flattenObject(null, m_currentRowNode, null);
//					LogUtilities.logDebug("-flattenObject 2------" + m_currentRowNode.toString(), m_logger);

				}

			}
			try {
				m_metadata.addAll(m_metadataHashtable.values());
				m_metadataHashtable.clear();
				// LogUtilities.logDebug("-m_metadataHashtable----" + m_metadata.toString(),
				// m_logger);
			} catch (Exception e) {
				LogUtilities.logError("Excption---------------" + e.getMessage(), m_logger);
			}
//            }else {
//                flattenObject(null, m_coloumNode, null);
//            }

			if (!m_currentRowNode.isObject() && !m_settings.m_format.equalsIgnoreCase("loadOnDemand")) {
				// The current row node in the root array node is not a JSON object.
				ErrorException ee = URDriver.s_QJMessages.createGeneralException(
						URMessageKey.UNSUPPORTED_JSON_FORMAT.name(),
						new String[] { "the current row node in the root array node is not a JSON " + "object" });
				LogUtilities.logError(ee, m_logger);
				throw ee;
			}

			// Set the size of the JSON document.
			m_rowCount = m_coloumNode.size();
		}

		// Flatten object from the row node.
		// flattenObject(null, m_currentRowNode, null);

		if (0 == m_currentRowNode.size()) {
			// Throw exception when the JSON document has the format as {} or [{}].
			ErrorException ee = URDriver.s_QJMessages.createGeneralException(URMessageKey.EMPTY_OBJECT.name());
			LogUtilities.logError(ee, m_logger);
			throw ee;
		}
	}

	public static String generateRandomString(int length) {
		String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
		StringBuilder sb = new StringBuilder(length);
		Random random = new Random();

		for (int i = 0; i < length; i++) {
			int index = random.nextInt(characters.length());
			char randomChar = characters.charAt(index);
			sb.append(randomChar);
		}

		return sb.toString();
	}

	public static String extractId(String input) {
		String[] parts = input.split("\\s+"); // Split based on one or more whitespace characters
		String id = null;

		// Check if the array has at least two elements (name and ID)
		if (parts.length >= 2) {
			id = parts[1];
		}

		return id;
	}

	public Map<String, JsonNode> extractTableData() {
		Map<String, JsonNode> map = new HashMap<>();
		try {
			JsonNode columnsNode = m_settings.m_rootNode.get("datasets");
			// .get(0);
//                .get("info")
//                .get("columns");

//		LogUtilities.logDebug("-Column Node---" + columnsNode.toString(),
//				m_logger);
			// Iterator<Entry<String, JsonNode>> firstNodeIterator =
			// columnsNode.get(0).fields();
			Iterator<JsonNode> datasetsIterator = columnsNode.iterator();
			while (datasetsIterator.hasNext()) {

				JsonNode datasetNode = datasetsIterator.next();
				JsonNode cNode = null;
				LogUtilities.logDebug("-CNODE-datasetNode----" + datasetNode.toString(), m_logger);
				String id = datasetNode.get("id").asText();
				LogUtilities.logDebug("-CNODE-  ID----" + id.toString(), m_logger);
				if (datasetNode.has("info") && datasetNode.get("info").has("columns")) {

					cNode = datasetNode.get("info").get("columns");
					LogUtilities.logDebug("-CNODE-----" + cNode.toString(), m_logger);
				}

				map.put(id, cNode);

			}
			LogUtilities.logDebug("-Map-----" + map.toString(), m_logger);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return map;

	}

	public void fetchData() throws IOException {
		if (m_tableIdentifier.getName() != null && m_settings.m_format.equals("loadOnDemand")) {
			LogUtilities.logDebug("---getModified URLS------" + m_settings.m_fieldsArr + "-", m_logger);
			for (int i = 0; i <= m_settings.m_fieldsArr.size() - 1; i++) {
				JsonNode nodeArr = m_settings.m_fieldsArr.get(i);
				if (nodeArr.has("url")) {
					if (nodeArr.get("url").toString() != null) {
						String url = nodeArr.get("url").toString().replace("\"", "");
						// String type= nodeArr.get("table").toString().replace("\"", "");
						String method = "GET";

						if (nodeArr.has("method"))
							method = nodeArr.get("method").toString().replace("\"", "");
						if (!url.isEmpty() && url != null
								&& (method == null || (method != null && method.equalsIgnoreCase("GET")))) {
							// for (String dbpath : urlList) {
							LogUtilities.logDebug("---GetMehtod-----" + method + "-", m_logger);
							JsonNode rootNode;
							if (!m_settings.m_tableData) {
								String dbpath = m_settings.m_baseUrl + url;
								LogUtilities.logDebug("---Modified URLS urTable------" + dbpath + "-", m_logger);
								rootNode = URCoreUtils.readDatafromJson(dbpath, m_logger, m_settings, null, null,
										false);
								m_settings.m_TablesData.put(url, rootNode);
							} else {
								rootNode = m_settings.m_TablesData.get(url);
							}

							Iterator<Entry<String, JsonNode>> firstDataIterator = rootNode.fields();
							int df = 0;

							while (firstDataIterator.hasNext()) {
								Entry<String, JsonNode> finalObject = firstDataIterator.next();
								LogUtilities.logDebug("---finalObject----" + finalObject, m_logger);

								JsonNode currentRowNode = finalObject.getValue();
								LogUtilities.logDebug("---m_currentRowNode- else------" + currentRowNode.toString(),
										m_logger);
								if (df == 1) {
									if (df == 1 && m_settings.m_format.equalsIgnoreCase("loadOnDemand")
											&& finalObject.getValue() != null
											&& !(0 == finalObject.getValue().size())) {
										JsonNode m_currentSchemaNode = finalObject.getValue();
										m_rootNodes = m_currentSchemaNode;
									}
								}
								df++;

							}
						}
						// TODO-AV: Move POST section on top and use GET as default.
						else { // Method is not GET, hence POST (for now)
							if (m_settings.str != null) {
								m_settings.m_columnIndexData = new HashMap<>();
								LogUtilities.logDebug("--- m_settings.selectedColumns Name-----" + m_settings.str,
										m_logger);
								JsonNode requestBody = null;
								String arrayName = null;
								String Id = null;
								if (nodeArr.has("arrayName")) {
									arrayName = nodeArr.get("arrayName").toString().replace("\"", "");
								}
								if (nodeArr.has("id")) {
									Id = nodeArr.get("id").toString().replace("\"", "");
								}
								if (nodeArr.has("requestBody"))
									requestBody = nodeArr.get("requestBody");
								if (requestBody != null && requestBody.has(Id)) {

									ObjectNode nodeObj = (ObjectNode) requestBody;
									nodeObj.remove(Id);
									String snapshotId = extractId(m_tableIdentifier.getSchema().toString());
									JsonNode newNode = new TextNode(snapshotId);
									nodeObj.set(Id, newNode);
									if (requestBody.has(arrayName)) {

										nodeObj.remove(arrayName);
									}
									ObjectMapper map = new ObjectMapper();
									try {
										JsonNode actualObj = map.readTree(m_settings.str);
										// JsonNode newNode = new TextNode(m_settings.str);
										nodeObj.set(arrayName, actualObj);
										LogUtilities.logDebug("--Selected Fields---" + requestBody.toString(),
												m_logger);

									} catch (JsonProcessingException e) {
										// TODO Auto-generated catch block
										LogUtilities.logError("-Exception" + e.getMessage(), m_logger);

										e.printStackTrace();
									}
//                                         JsonNode newNode = new TextNode(m_settings.str);
//                                         nodeObj.set("seleted_fields",newNode);
//                                         LogUtilities.logDebug("--- seleted_fields---"+requestBody, m_logger);
									// }
								}
								JsonNode rootNode;
								if (!m_settings.m_tableData) {
									// if(m_settings.str!=null) {
									String dbpath = m_settings.m_baseUrl + url;
									LogUtilities.logDebug("---Request Body- before Request---" + requestBody, m_logger);

									rootNode = URCoreUtils.readDatafromJson(dbpath, m_logger, m_settings, method,
											requestBody, false);
									LogUtilities.logDebug("--Response Post Request---" + rootNode, m_logger);
									m_settings.m_TablesData.put(url, rootNode);
									// }

								} else {
									rootNode = m_settings.m_TablesData.get(url);
								}

								Iterator<Entry<String, JsonNode>> firstDataIterator = rootNode.fields();
								int df = 0;

								while (firstDataIterator.hasNext()) {
									Entry<String, JsonNode> finalObject = firstDataIterator.next();
//m_columnIndexData
									JsonNode currentRowNode = finalObject.getValue();
									if (df == 1) {
										for (int in = 0; in <= currentRowNode.size() - 1; in++) {
											JsonNode nodes = currentRowNode.get(in);
											if (nodes.has("colName")) {
												if (nodes.get("colName").toString() != null) {
													if (m_settings.m_columnData.size() > 0) {
														m_settings.m_columnIndexData.put(m_settings.m_columnData.get(
																nodes.get("colName").toString().replace("\"", "")), in);
													} else {

														m_settings.m_columnIndexData.put(
																nodes.get("colName").toString().replace("\"", ""), in);
													}
												}
											}
										}
										LogUtilities.logDebug(
												"-m_settings.m_columnIndexData::::---" + m_settings.m_columnIndexData,
												m_logger);

									}
									if (df == 2) {

										if (df == 2 && m_settings.m_format.equalsIgnoreCase("loadOnDemand")

												&& finalObject.getValue() != null
												&& !(0 == finalObject.getValue().size())) {
											JsonNode m_currentSchemaNode = finalObject.getValue();
											m_rootNodes = m_currentSchemaNode;
										}
									}
									df++;

								}
							}

						}
					}
				}
			}
			// m_settings.m_tableData=true;
		}
	}
}
