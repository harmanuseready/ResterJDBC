// =================================================================================================
///  @file QJMetadataHelper.java
///
///  Definition of the Class QJMetadataHelper
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.dataengine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simba.dsi.core.interfaces.IConnection;
import com.simba.dsi.dataengine.utilities.MetadataSourceColumnTag;
import com.simba.dsi.dataengine.utilities.MetadataSourceID;
import com.simba.sqlengine.dsiext.dataengine.IMetadataHelper;
import com.simba.sqlengine.dsiext.dataengine.Identifier;
import com.simba.support.ILogger;
import com.simba.support.IMessageSource;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ErrorException;
import com.useready.rester.Settings;
import com.useready.rester.core.URConnection;
import com.useready.rester.core.URCoreUtils;

/**
 * QuickJson metadata helper implementation.
 *
 * This class provides a list of tables that are used to construct default
 * implementations of the metadata sources for tables and columns.
 *
 * Each table or procedure will be opened to obtain metadata about it, although
 * data will not be fetched.
 *
 * Note that use of this class is optional, and if an implementation is returned
 * via IDataEngine.MakeNewMetadataTable(), the default implementation will _NOT_
 * be used.
 */
public class URMetadataHelper implements IMetadataHelper {
	/*
	 * Instance
	 * variable(s)==================================================================
	 * ======
	 */

	/**
	 * Iterator to iterate through all catalogs.
	 */
	private Iterator<String> m_catalogIter;

	/**
	 * The current catalog gotten from iterator.
	 */
	private String m_currentCatalog;

	/**
	 * Iterator to iterate through all schemas in a catalog.
	 */
	private Iterator<String> m_schemaIter;

	/**
	 * The current schema gotten from iterator.
	 */
	private String m_currentSchema;

	/**
	 * Iterator to iterate through all tables based on schema and catalog.
	 */
	private Iterator<String> m_tableIter;

	/**
	 * The current name of table gotten from iterator.
	 */
	private String m_currentTable;

	/**
	 * The settings for the driver.
	 */
	private Settings m_settings;

	/*
	 * Constructor(s)
	 * =============================================================================
	 * =
	 */

	/**
	 * Constructor
	 *
	 * @param parentConnection The parent connection.
	 */

	int test = 0;

	public URMetadataHelper(IConnection parentConnection, Map<MetadataSourceColumnTag, String> restrictions,
			MetadataSourceID metadataSourceId) {
		LogUtilities.logFunctionEntrance(parentConnection.getConnectionLog(), parentConnection);
	//restrictions.put(MetadataSourceColumnTag.CATALOG_NAME, "2022-02-24");
	// restrictions.put(MetadataSourceColumnTag.SCHEMA_NAME, "PROTIIDE OUTPUT");
		LogUtilities.logDebug("metadataSourceId ::::::-" + metadataSourceId,
				parentConnection.getConnectionLog());
		m_settings = ((URConnection) parentConnection).m_settings;
//		LogUtilities.logDebug("-URMetadataHelper------" + m_settings.m_tableMap.toString(),
//				parentConnection.getConnectionLog());
		m_catalogIter = m_settings.m_tableMap.keySet().iterator();
		Map<String, List<String>> tableMapInCurrentCatalog;
		if (restrictions.size() == 0) {
			LogUtilities.logDebug("--restrictions size---------" + restrictions.toString(),
					parentConnection.getConnectionLog());

			if (m_catalogIter.hasNext()) {
				m_currentCatalog = m_catalogIter.next();
				LogUtilities.logDebug("--m_currentCatalog--" + m_currentCatalog.toString(),
						parentConnection.getConnectionLog());

				tableMapInCurrentCatalog = m_settings.m_tableMap.get(m_currentCatalog);

			} else {
				tableMapInCurrentCatalog = null;
			}

			if (null != tableMapInCurrentCatalog) {
				// Schema iterator that iterates through all schemas with catalog.
				m_schemaIter = tableMapInCurrentCatalog.keySet().iterator();

				List<String> tableMapInCurrentSchema;
				if (m_schemaIter.hasNext()) {
					m_currentSchema = m_schemaIter.next();
					LogUtilities.logDebug("--m_currentSchema--" + m_currentSchema.toString(),
							parentConnection.getConnectionLog());
					tableMapInCurrentSchema = tableMapInCurrentCatalog.get(m_currentSchema);
					LogUtilities.logDebug("--Table MAP--" + m_settings.m_tableMap.toString(),
							parentConnection.getConnectionLog());
				} else {
					tableMapInCurrentSchema = null;
					m_schemaIter = null;
					m_tableIter = null;
				}

				if (null != tableMapInCurrentSchema) {
					// Table iterator that iterates through all tables with catalog and schema.
					m_tableIter = tableMapInCurrentSchema.iterator();
				}
			}
		} else {
			LogUtilities.logDebug("--restrictions size greater than 0---------" + m_settings.m_tableMap,
					parentConnection.getConnectionLog());

			try {
				String catalog = restrictions.get(MetadataSourceColumnTag.CATALOG_NAME);
				if (restrictions.get(MetadataSourceColumnTag.CATALOG_NAME) != null
						&& metadataSourceId != MetadataSourceID.COLUMNS
						&& metadataSourceId != MetadataSourceID.TYPE_INFO) {
					m_currentCatalog = restrictions.get(MetadataSourceColumnTag.CATALOG_NAME);
					m_currentSchema = restrictions.get(MetadataSourceColumnTag.SCHEMA_NAME);
					LogUtilities.logDebug("--m_currentSchema---------" + m_currentSchema,
							parentConnection.getConnectionLog());
					JsonNode rootNode = null;
					if (m_settings.m_format.equals("oneCall")) {
						//String schemaUrl = m_settings.m_baseUrl + m_settings.m_schema;
//						LogUtilities.logDebug("--m_currentSchema url onecall--------" + schemaUrl,
//								parentConnection.getConnectionLog());
//						rootNode = URCoreUtils.readDatafromJson(schemaUrl, parentConnection.getConnectionLog(),
//								m_settings, null, null, false);
					} else {
						// rootNode=m_settings.m_schemaLiST;
						if (!m_settings.m_schemalist) {
							String schemaUrl;
							if (m_settings.m_schema.contains("{")) {
								String url = m_settings.m_schema.replaceAll("\\{.*?\\}", catalog);
								schemaUrl = m_settings.m_baseUrl + url;

							} else {
								schemaUrl = m_settings.m_baseUrl + m_settings.m_schema;
							}

							LogUtilities.logDebug("--SchemaURL----" + schemaUrl.toString(),
									parentConnection.getConnectionLog());
							m_settings.m_schemaLiST = URCoreUtils.readDatafromJson(schemaUrl,
									parentConnection.getConnectionLog(), m_settings, null, null, false);
							rootNode = m_settings.m_schemaLiST;
							m_settings.m_schemalist = true;
						} else {
							rootNode = m_settings.m_schemaLiST;
						}

					}

					// JsonNode m_currentRowNode = rootNode.get(0);

					JsonNode m_currentRowNode = rootNode.get(0);
					if (m_currentCatalog != null && m_currentSchema == null
							&& (metadataSourceId == MetadataSourceID.CATALOG_SCHEMA_ONLY
									|| metadataSourceId == MetadataSourceID.SCHEMA_ONLY)) {
						if (rootNode.isArray()) {
//							LogUtilities.logDebug("--Schema List is Array------" + rootNode.toString(),
//									parentConnection.getConnectionLog());
							m_settings.m_tableMap = createSchema(rootNode, m_currentCatalog, m_settings.m_tableMap,
									m_currentSchema);
						} else {
//							LogUtilities.logDebug("--Schema List is Object------" + rootNode.toString(),
//									parentConnection.getConnectionLog());

							m_settings.m_tableMap = createSchemaObject(rootNode, m_currentCatalog,
									m_settings.m_tableMap, m_currentSchema, parentConnection.getConnectionLog());
						}

						// restrictions.put(MetadataSourceColumnTag.SCHEMA_NAME, "2022-01-03");
						// restrictions.put(MetadataSourceColumnTag.SCHEMA_NAME, "PROTIIDE OUTPUT");

					} else if (m_currentCatalog != null && metadataSourceId == MetadataSourceID.TABLES) {
						if (m_currentSchema == null) {
							Map<String, List<String>> m_schema = m_settings.m_tableMap.get(m_currentCatalog);
							for (Map.Entry<String, List<String>> entry : m_schema.entrySet()) {

								restrictions.put(MetadataSourceColumnTag.SCHEMA_NAME, entry.getKey());

							}
						}
						LogUtilities.logDebug("--MetadataSourceID---------" + metadataSourceId,
								parentConnection.getConnectionLog());
						test++;

						m_currentSchema = restrictions.get(MetadataSourceColumnTag.SCHEMA_NAME);
						LogUtilities.logDebug( m_currentSchema,parentConnection.getConnectionLog());
						if (m_settings.m_format.equals("loadOnDemand") && m_settings.m_tablesArr != null) {
//							LogUtilities.logDebug("--Tables ---------" + m_settings.m_tablesArr.toString(),
//									parentConnection.getConnectionLog());
							m_settings.m_tableMap = readTableNameFromArray(m_settings.m_tablesArr, m_currentCatalog,
									m_settings.m_tableMap, m_currentSchema, parentConnection.getConnectionLog());
						} else {
							String tableUrl = m_settings.m_baseUrl + m_settings.m_tables;
							JsonNode rootTable = URCoreUtils.readDatafromJson(tableUrl,
									parentConnection.getConnectionLog(), m_settings, null, null, false);
							if (rootTable.isObject()) {
//								LogUtilities.logDebug("--Tables List is Object------" + rootTable.toString(),
//										parentConnection.getConnectionLog());

								m_settings.m_tableMap = createTable(rootTable, m_currentCatalog, m_settings.m_tableMap,
										m_currentSchema, parentConnection.getConnectionLog());

							} else {
//								LogUtilities.logDebug("--Tables List is Array------" + rootTable.toString(),
//										parentConnection.getConnectionLog());

								m_settings.m_tableMap = createTablesArray(rootTable, m_currentCatalog,
										m_settings.m_tableMap, m_currentSchema);
							}
						}

						// }

					}
					tableMapInCurrentCatalog = m_settings.m_tableMap.get(m_currentCatalog);
//					LogUtilities.logDebug("--After Table creation------" + tableMapInCurrentCatalog.toString(),
//							parentConnection.getConnectionLog());
					// }
				} else {
					LogUtilities.logDebug("--metadataSourceId coloum------" + metadataSourceId.toString(),
							parentConnection.getConnectionLog());
					if (metadataSourceId.equals(MetadataSourceID.COLUMNS)) {
						if (restrictions.get(MetadataSourceColumnTag.CATALOG_NAME) != null
								/// && metadataSourceId != MetadataSourceID.COLUMNS
								&& metadataSourceId != MetadataSourceID.TYPE_INFO) {
							m_currentCatalog = restrictions.get(MetadataSourceColumnTag.CATALOG_NAME);
							m_currentSchema = restrictions.get(MetadataSourceColumnTag.SCHEMA_NAME);
							LogUtilities.logDebug("--m_currentSchema-else--------" + m_currentSchema + "---"
									+ m_settings.m_schemalist, parentConnection.getConnectionLog());
							JsonNode rootNode;
							if (!m_settings.m_schemalist && m_settings.m_format.equals("loadOnDemand") ) {
								String schemaUrl;
								if (m_settings.m_schema.contains("{")) {
									String url = m_settings.m_schema.replaceAll("\\{.*?\\}", catalog);
									schemaUrl = m_settings.m_baseUrl + url;

								} else {
									schemaUrl = m_settings.m_baseUrl + m_settings.m_schema;
								}

								LogUtilities.logDebug("--SchemaURL----" + schemaUrl.toString(),
										parentConnection.getConnectionLog());
								m_settings.m_schemaLiST = URCoreUtils.readDatafromJson(schemaUrl,
										parentConnection.getConnectionLog(), m_settings, null, null, false);
								rootNode = m_settings.m_schemaLiST;
								m_settings.m_schemalist = true;
							} else {
								rootNode = m_settings.m_schemaLiST;
							}
//							LogUtilities.logDebug("--m_currentSchema-Root Node--------" + rootNode.toString(),
//									parentConnection.getConnectionLog());
							JsonNode m_currentRowNode = rootNode.get(0);
							if (m_currentCatalog != null && m_currentSchema != null) {
								if (rootNode.isArray()) {
//									LogUtilities.logDebug("--Schema List is Array------" + rootNode.toString(),
//											parentConnection.getConnectionLog());
									m_settings.m_tableMap = createSchema(rootNode, m_currentCatalog,
											m_settings.m_tableMap, m_currentSchema);
								} else {
//									LogUtilities.logDebug("--Schema List is Object------" + rootNode.toString(),
//											parentConnection.getConnectionLog());

									m_settings.m_tableMap = createSchemaObject(rootNode, m_currentCatalog,
											m_settings.m_tableMap, m_currentSchema,
											parentConnection.getConnectionLog());
									m_settings.m_tableMap = readTableNameFromArray(m_settings.m_tablesArr,
											m_currentCatalog, m_settings.m_tableMap, m_currentSchema,
											parentConnection.getConnectionLog());
								}

							} else if (m_currentCatalog != null && metadataSourceId == MetadataSourceID.TABLES) {
								if (m_currentSchema == null) {
									Map<String, List<String>> m_schema = m_settings.m_tableMap.get(m_currentCatalog);
									for (Map.Entry<String, List<String>> entry : m_schema.entrySet()) {

										restrictions.put(MetadataSourceColumnTag.SCHEMA_NAME, entry.getKey());

									}
								}
//								LogUtilities.logDebug("--MetadataSourceID---------" + metadataSourceId,
//										parentConnection.getConnectionLog());
								test++;

								m_currentSchema = restrictions.get(MetadataSourceColumnTag.SCHEMA_NAME);
								if (m_settings.m_format.equals("loadOnDemand") && m_settings.m_tablesArr != null) {
									LogUtilities.logDebug("--Tables ---------" + m_settings.m_tablesArr.toString(),
											parentConnection.getConnectionLog());
									m_settings.m_tableMap = readTableNameFromArray(m_settings.m_tablesArr,
											m_currentCatalog, m_settings.m_tableMap, m_currentSchema,
											parentConnection.getConnectionLog());
								} else {
									String tableUrl = m_settings.m_baseUrl + m_settings.m_tables;
									JsonNode rootTable = URCoreUtils.readDatafromJson(tableUrl,
											parentConnection.getConnectionLog(), m_settings, null, null, false);
									if (rootTable.isObject()) {
										LogUtilities.logDebug("--Tables List is Object------" + rootTable.toString(),
												parentConnection.getConnectionLog());

										m_settings.m_tableMap = createTable(rootTable, m_currentCatalog,
												m_settings.m_tableMap, m_currentSchema,
												parentConnection.getConnectionLog());

									} else {

										m_settings.m_tableMap = createTablesArray(rootTable, m_currentCatalog,
												m_settings.m_tableMap, m_currentSchema);
									}
								}

								// }

							}
							tableMapInCurrentCatalog = m_settings.m_tableMap.get(m_currentCatalog);
							LogUtilities.logDebug("--After Table creation------" + tableMapInCurrentCatalog.toString(),
									parentConnection.getConnectionLog());
							// }
						}
					}

					if (m_catalogIter.hasNext()) {

						m_currentCatalog = m_catalogIter.next();
						tableMapInCurrentCatalog = m_settings.m_tableMap.get(m_currentCatalog);
					} else {
						tableMapInCurrentCatalog = null;
					}
				}

				if (null != tableMapInCurrentCatalog && !m_settings.m_format.equals("loadOnDemand")
						&& !m_settings.m_format.equals("oneCall")) {
					// Schema iterator that iterates through all schemas with catalog.
					m_schemaIter = tableMapInCurrentCatalog.keySet().iterator();

					List<String> tableMapInCurrentSchema;
					if (m_schemaIter.hasNext()) {
						m_currentSchema = m_schemaIter.next();
						tableMapInCurrentSchema = tableMapInCurrentCatalog.get(m_currentSchema);
						
					} else {
						tableMapInCurrentSchema = null;
					}

					if (null != tableMapInCurrentSchema && !tableMapInCurrentSchema.isEmpty()) {
						m_tableIter = tableMapInCurrentSchema.iterator();
					}
				}

			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();

			}

		}

	}

	private Map<String, Map<String, List<String>>> createSchema(JsonNode schemaList, String catalog,
			Map<String, Map<String, List<String>>> tableMapping, String schema) {
		Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
		for (int i = 0; i <= schemaList.size() - 1; i++) {

			String schemaName = null;
			schemaName = schemaList.get(i).toString().replace("\"", "");
			currentCatalog.put(schemaName, new ArrayList<String>());
			tableMapping.put(catalog, currentCatalog);
		}
		return tableMapping;
	}

	private Map<String, Map<String, List<String>>> createTablesArray(JsonNode schemaList, String catalog,
			Map<String, Map<String, List<String>>> tableMapping, String schema) {
		Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
		Map<String, List<String>> tableList = tableMapping.get(catalog);
		List<String> currentSchema = new ArrayList<String>();
		for (int i = 0; i <= schemaList.size() - 1; i++) {

			String schemaName = null;
			currentSchema.add(schemaList.get(i).toString().replace("\"", ""));
			tableList.put(schema, currentSchema);
			tableMapping.put(catalog, tableList);
		}
		return tableMapping;
	}

	private Map<String, Map<String, List<String>>> readTableNameFromArray(JsonNode tablesList, String catalog,
			Map<String, Map<String, List<String>>> tableMapping, String schema, ILogger iLogger) {
		LogUtilities.logDebug("--readTableNameFromArray-- tablesList------" + tablesList.toString(), iLogger);
		for (int i = 0; i <= tablesList.size() - 1; i++) {
			JsonNode node = tablesList.get(i);
			if (node.has("url")) {
				if (node.get("url").toString() != null) {
					String DpAttr = null;
					String url = node.get("url").toString().replace("\"", "");
					String tablePre = node.get("TablePrefix").toString().replace("\"", "");
					m_settings.m_measureDataType = node.get("type").toString().replace("\"", "");
					String fieldName = node.get("fieldName").toString().replace("\"", "");
					String dataType=null;
					if(node.has("fieldType") && node.get("fieldType")!=null && node.get("fieldType").toString().trim()!="") {
				  dataType = node.get("fieldType").toString().replace("\"", "");
					}

					if (node.has("DpAttr")) {
						DpAttr = node.get("DpAttr").toString().replace("\"", "");
						LogUtilities.logDebug("--DpAttr----" + DpAttr, iLogger);
					}

					if (node.get("displayName").toString() != null) {
						String displayName = node.get("displayName").toString().replace("\"", "");
						if (displayName.contains("_")) {
							List<String> ar = Arrays.asList(displayName.split("_"));
							m_settings.displayNameComb.put(url, ar);
						} else {
							List<String> str = Arrays.asList(displayName);
							m_settings.displayNameComb.put(url, str);
						}
						Map<String, Map<String, List<String>>> tableMap = new TreeMap<String, Map<String, List<String>>>();
						Map<String, Map<String, List<String>>> tableMap2 = new TreeMap<String, Map<String, List<String>>>();
						// for(String tableName:str) {
						JsonNode rootTable;
						if (!m_settings.m_tableList) {
							String tableUrl = m_settings.m_baseUrl + url;
							rootTable = URCoreUtils.readDatafromJson(tableUrl, iLogger, m_settings, null, null, false);
							m_settings.m_Tables.put(url, rootTable);
						} else {
							rootTable = m_settings.m_Tables.get(url);

						}
						// Display Table Name from the Dimesions and Measures
						if (rootTable.isObject()) {
							m_settings.m_tableMap = readTableFromJSONObj(rootTable, m_currentCatalog,
									m_settings.m_tableMap, m_currentSchema, iLogger, tablePre, displayName, fieldName,dataType,
									url);
							LogUtilities.logDebug("--rootTable is tableMap-------" + m_settings.m_tableMap.toString(),
									iLogger);

						} else {

							m_settings.m_tableMap = createTablesArray(rootTable, m_currentCatalog,
									m_settings.m_tableMap, m_currentSchema);

						}
					}

				}

			}

		}
		m_settings.m_tableList = true;

		return m_settings.m_tableMap;

	}

	private Map<String, Map<String, List<String>>> readTableFromJSONObj(JsonNode tablesList, String catalog,
			Map<String, Map<String, List<String>>> tableMapping, String schema, ILogger iLogger, String tablePre,
			String displayName, String fieldName,String dataType, String dept) {
		Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
		JsonNode nodes = tablesList;
		Map<String, List<String>> tableList = tableMapping.get(catalog);
		if (displayName.contains("_")) {
			String[] s = displayName.split("_");
			displayName = s[0];
		}
		try {
			int c = 0;
			if (tablesList.has("di")) {
				tablesList.get("di");
			} else if (tablesList.has("me")) {
				tablesList.get("me");
			}
			LogUtilities.logDebug("-readTableFromJSONObj -Tables List---------" + tableList.toString(), iLogger);
			Iterator<Entry<String, JsonNode>> objectIterator = tablesList.fields();
			String cobDate = "";
			String schemaID = "";
			String tableName = "";
			String deptName = "";

			Map<String, String> map = new HashMap<String, String>();
			while (objectIterator.hasNext()) {

				Entry<String, JsonNode> object = objectIterator.next();
				if (c == 1) {
					JsonNode schemaList = object.getValue();
					for (int i = 0; i <= schemaList.size() - 1; i++) {
						JsonNode node = schemaList.get(i);

						if (node.has("colName")) {

							if (node.get("colName").toString() != null
									&& (node.get("colName").toString().replace("\"", "")).equals(displayName)) {
								tableName = node.get("idx").toString().replace("\"", "");
							}
//							if (node.get("colName").toString() != null
//									&& (node.get("colName").toString().replace("\"", "")).equals(dept)) {
//								deptName = node.get("idx").toString().replace("\"", "");
//							}

						}
					}
				}
				StringBuilder name = new StringBuilder();
				object.getKey();
				if (c == 2) {
					JsonNode snapShotList = object.getValue();
					List<String> currentSchema = tableList.get(schema);
					LogUtilities.logDebug("-readTableFromJSONObj -currentSchema----" + currentSchema.toString(),
							iLogger);
					for (int a = 0; a <= snapShotList.size() - 1; a++) {

						JsonNode schemaNode = snapShotList.get(a);

						if (!(tableList.get(schema).toString().replaceAll("\\[", "").replaceAll("\\]", ""))
								.equals(schemaNode.get(Integer.parseInt(tableName)).toString().replace("\"", ""))
								&& !currentSchema.contains(schemaNode.get(Integer.parseInt(tableName)).toString()
										.replace("\"", "").replaceAll("\\[", "").replaceAll("\\]", ""))) {
							String tname = schemaNode.get(Integer.parseInt(tableName)).toString().replace("\"", "")
									.replaceAll("\\[", "").replaceAll("\\]", "");
//							String depName=null;
//							if(deptName!="" && deptName!=null && !deptName.isEmpty()) {
//								depName = schemaNode.get(Integer.parseInt(deptName)).toString().replace("\"", "")
//										.replaceAll("\\[", "").replaceAll("\\]", "");	
//							}
//							LogUtilities.logDebug("--Table Mapping depName------" + depName, iLogger);
//							LogUtilities.logDebug("--Table Mapping nodes------" + nodes.toString(), iLogger);

							if (tablePre != null && !tablePre.isEmpty()) {
//								if(deptName!=null && !depName.isEmpty()) {
//									tname = tablePre+"." + deptName + "." + tname;
//								}else {
								tname = tablePre + "." + tname;
//								}

							}
							// m_settings.m_tablesList.put(tname, nodes);
							// m_settings.m_tablesList.put(tname, nodes);
							List<String> list = new ArrayList<String>();
							list.add(fieldName);
							list.add(displayName);
                            list.add(dataType); 
							list.add(dept);

							list.add(nodes.toString());

							m_settings.m_tablesLists.put(tname, list);
//							currentSchema.add(tname);
//							tableList.put(schema, currentSchema);
//							tableMapping.put(catalog, tableList);
//							LogUtilities.logDebug("--Table Mapping------" + tableMapping.toString(), iLogger);
//							LogUtilities.logDebug("--Tables List------" + m_settings.m_tablesList.toString(), iLogger);
						}
						if (m_settings.m_TableName != null && !m_settings.m_TableName.isEmpty()
								&& !currentSchema.contains(m_settings.m_TableName)) {
							currentSchema.add(m_settings.m_TableName);
							tableList.put(schema, currentSchema);
							tableMapping.put(catalog, tableList);
						}
						break;
					}
				}

				c++;

			}

		} catch (Exception e) {
			LogUtilities.logError("-Exception-----" + e.getMessage(), iLogger);
		}
		LogUtilities.logDebug("--Tables List---------" + tableMapping.toString(), iLogger);
		return tableMapping;
	}

	private Map<String, Map<String, List<String>>> createTable(JsonNode tablesList, String catalog,
			Map<String, Map<String, List<String>>> tableMapping, String schema, ILogger iLogger) {
		Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
		Map<String, List<String>> tableList = tableMapping.get(catalog);
		LogUtilities.logDebug("--Tables List---------" + tableList.toString(), iLogger);
		int c = 0;
		Iterator<Entry<String, JsonNode>> objectIterator = tablesList.fields();
		String cobDate = "";
		String schemaID = "";
		String tableName = "";
		Map<String, String> map = new HashMap<String, String>();
		while (objectIterator.hasNext()) {

			Entry<String, JsonNode> object = objectIterator.next();
			if (c == 0) {
				JsonNode schemaList = object.getValue();
				for (int i = 0; i <= schemaList.size() - 1; i++) {
					JsonNode node = schemaList.get(i);

					if (node.has("colName")) {
						if (node.get("colName").toString() != null
								&& (node.get("colName").toString().replace("\"", "")).equals("CobDate")) {
							cobDate = node.get("idx").toString().replace("\"", "");
						}

						if (node.get("colName").toString() != null
								&& (node.get("colName").toString().replace("\"", "")).equals("Schema")) {
							schemaID = node.get("idx").toString().replace("\"", "");
						}

						if (node.get("colName").toString() != null
								&& (node.get("colName").toString().replace("\"", "")).equals("Name")) {
							tableName = node.get("idx").toString().replace("\"", "");
						}

					}
				}
			}
			StringBuilder name = new StringBuilder();
			object.getKey();
			if (c == 1) {
				JsonNode snapShotList = object.getValue();
				List<String> currentSchema = new ArrayList<String>();

				for (int a = 0; a <= snapShotList.size() - 1; a++) {

					JsonNode schemaNode = snapShotList.get(a);
					if (schema.equals(schemaNode.get(Integer.parseInt(cobDate)).toString().replace("\"", "")) && catalog
							.equals(schemaNode.get(Integer.parseInt(schemaID)).toString().replace("\"", ""))) {

						if (!(tableList.get(schema).toString().replaceAll("\\[", "").replaceAll("\\]", ""))
								.equals(schemaNode.get(Integer.parseInt(tableName)).toString().replace("\"", ""))) {
							LogUtilities.logDebug(
									"--Tables List---Table Name not exist------" + tableList.get(schema).toString(),
									iLogger);
							currentSchema.add(schemaNode.get(Integer.parseInt(tableName)).toString().replace("\"", "")
									.replaceAll("\\[", "").replaceAll("\\]", ""));
							tableList.put(schema, currentSchema);
							tableMapping.put(catalog, tableList);
						}

					}

				}
			}

			c++;

		}
		LogUtilities.logDebug("--Tables List---------" + tableMapping.toString(), iLogger);
		return tableMapping;
	}

	private Map<String, Map<String, List<String>>> createSchemaObject(JsonNode nodeList, String catalog,
			Map<String, Map<String, List<String>>> tableMapping, String schema, ILogger iLogger) {
		Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>();
		if (m_settings.m_schemaOrder != null && m_settings.m_schemaOrder != "" && !m_settings.m_schemaOrder.isEmpty()
				&& m_settings.m_schemaOrder.equalsIgnoreCase("desc")) {
			currentCatalog = new TreeMap<>(Comparator.reverseOrder());
		} else {
			currentCatalog = new TreeMap<String, List<String>>();
		}
		Map<String, List<String>> tableList = tableMapping.get(catalog);

		int c = 0;
		if (nodeList.has("sna")) {
			nodeList = nodeList.get("sna");
		}
		try {
//			LogUtilities.logDebug("-nodeList List--------" + nodeList.toString(), iLogger);
			Iterator<Entry<String, JsonNode>> objectIterator = nodeList.fields();
			// Create a Map to store the mapping of col names to their indices in the schema
			List<Integer> colNameToIndexMap = new ArrayList<Integer>();
			String cob_Date = "";
			Map<String, String> map = new HashMap<String, String>();
			while (objectIterator.hasNext()) {

				Entry<String, JsonNode> object = objectIterator.next();
				//LogUtilities.logDebug(" m_settings.m_SchemaName -----" + m_settings.m_SchemaName, iLogger);
				if (c == 1) {
					JsonNode schemaList = object.getValue();
					for (int i = 0; i <= schemaList.size() - 1; i++) {
						JsonNode node = schemaList.get(i);
						// LogUtilities.logDebug("-C -----" + c +"----"+node.toString(), iLogger);
						if (node.has("colName")) {
							for (int tb = 0; tb < m_settings.m_SchemaName.size(); tb++) {
								if (node.get("colName").toString() != null && m_settings.m_SchemaName.get(tb)
										.equals(node.get("colName").toString().replace("\"", ""))) {
									// colNameToIndexMap.add(Integer.parseInt(node.get("idx").toString().replace("\"",
									// "")));
									// LogUtilities.logDebug("-C -----" + c +"----"+node.toString(), iLogger);
									m_settings.m_SchemaName.set(tb, (node.get("idx").toString().replace("\"", "")));
								}
								if (node.get("colName").toString() != null
										&& (node.get("colName").toString().replace("\"", "")).equals("CobDate")) {
									cob_Date = node.get("idx").toString().replace("\"", "");
								}
							}
						}
					}
				}
				StringBuilder name = new StringBuilder();
				object.getKey();
				if (c == 2) {
					//LogUtilities.logDebug("-m_settings.m_SchemaName----" + m_settings.m_SchemaName.toString(), iLogger);
					JsonNode snapShotList = object.getValue();
					// LogUtilities.logDebug("-Schema- list-"+c+"---- "+ colNameToIndexMap, iLogger);
					List<String> currentSchema = new ArrayList<String>();

					for (int a = 0; a <= snapShotList.size() - 1; a++) {
						StringBuilder concatenatedValues = new StringBuilder();
						JsonNode schemaNode = snapShotList.get(a);
						if (catalog.equals(schemaNode.get(Integer.parseInt(cob_Date)).toString().replace("\"", ""))) {
							// LogUtilities.logDebug("-Inside IF-- "+ colNameToIndexMap, iLogger);
							for (String colIndex : m_settings.m_SchemaName) {
								// LogUtilities.logDebug("-colIndex--" + colIndex.toString(), iLogger);
								concatenatedValues
										.append(schemaNode.get(Integer.parseInt(colIndex.toString().replace("\"", ""))))
										.append(" ");
							}
							currentCatalog.put(concatenatedValues.toString().replace("\"", ""),
									new ArrayList<String>());
							//LogUtilities.logDebug("-Schema concatenatedValues----" + currentCatalog.toString(), iLogger);

							tableMapping.put(catalog, currentCatalog);

						}

					}
				}

				c++;

			}
		} catch (Exception e) {
			LogUtilities.logError("Exception---" + e.getMessage(), iLogger);
			e.printStackTrace();
		}

		LogUtilities.logDebug("--Tables List--m-------" + tableMapping.toString(), iLogger);
		return tableMapping;
	}
	/*
	 * Method(s)
	 * =============================================================================
	 * ======
	 */

	/**
	 * Get the next stored procedure in the data source.
	 *
	 * When this function is first called it should return the first procedure in
	 * the data source, and on each successive call it should return the next
	 * procedure. When there are no more procedures, it should return false.
	 *
	 * @param procedure The class to fill with information about the next stored
	 *                  procedure.
	 *
	 * @return True if there was another stored procedure; false otherwise.
	 */
	@Override
	public boolean getNextProcedure(Identifier procedure) {
		// Not implemented in this example.
		return false;
	}

	/**
	 * Get the next table in the data source.
	 *
	 * When this function is first called it should return the first table in the
	 * data source, and on each successive call it should return the next table.
	 * When there are no more tables, it should return false.
	 *
	 * @param table The class to fill with information about the next table.
	 *
	 * @return True if there was another table; false otherwise.
	 */
	@Override
	public boolean getNextTable(Identifier table) {

		// If any iterator is null then the DBF has no tables with catalog and schema.
//		if ((null == m_catalogIter) || (null == m_schemaIter) || (null == m_tableIter)) {
//			return false;
//		}

		if (updateTable()) {

			table.setCatalog(m_currentCatalog);
			table.setSchema(m_currentSchema);
			if (m_currentTable == null) {

				table.setName("");
			} else {
				table.setName(m_currentTable);
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Function to update the catalog from the iterator.
	 *
	 * @return True if it was able to update catalog and any sub iterator. False if
	 *         there are no more catalogs in iterator.
	 */
	private boolean updateCatalog() {

		if (m_catalogIter != null && m_catalogIter.hasNext()) {
			m_currentCatalog = m_catalogIter.next();

			if (m_settings.m_tableMap.get(m_currentCatalog) != null
					&& m_settings.m_tableMap.get(m_currentCatalog).size() > 0) {

				m_schemaIter = m_settings.m_tableMap.get(m_currentCatalog).keySet().iterator();
				return updateSchema();
			} else {
				return true;
			}

		} else {
			return false;
		}
	}

	/**
	 * Function to update the schema from the iterator.
	 *
	 * @return True if it was able to update schema and any other sub iterator.
	 *         False if there are no more catalogs left.
	 */
	private boolean updateSchema() {
		if (m_schemaIter != null && m_schemaIter.hasNext()) {

			m_currentSchema = m_schemaIter.next();

			if (m_settings.m_tableMap.get(m_currentCatalog).get(m_currentSchema) != null
					&& m_settings.m_tableMap.get(m_currentCatalog).get(m_currentSchema).size() > 0) {
				m_tableIter = m_settings.m_tableMap.get(m_currentCatalog).get(m_currentSchema).iterator();
				return updateTable();
			} else {
				m_currentTable = null;
				return true;
			}

		} else {
			return updateCatalog();
		}
	}

	/**
	 * Function to update the table from the iterator. The algorithm to updateTable
	 * is a recursive algorithm that will check to see if there are tables left to
	 * read in the iterator. If there is then it will just move the iterator and
	 * retrieve the table name. If there are no other tables then it will call
	 * update schema which will move the schema to the next one in the catalog,
	 * update the table iterator and then call update table. If there is no schemas
	 * then it will move the catalog to the next catalog and then update schema
	 * iterator and call the schema method which will update the table iterator.
	 *
	 * @return True if it was able to update currentTable. False if there are not
	 *         catalogs left.
	 */
	private boolean updateTable() {
		if (m_tableIter != null && m_tableIter.hasNext()) {
			m_currentTable = m_tableIter.next();
			return true;
		} else {

			return updateSchema();
		}
	}

	public void setString(List<String> str, ILogger iLogger) {
		LogUtilities.logDebug("-setString--" + str.toString(), iLogger);
	}
}
