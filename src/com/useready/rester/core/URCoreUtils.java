///  @file QJCoreUtils.java
///
///  Definition of the Class QJCoreUtils
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.codec.binary.Base64;
import org.apache.cxf.headers.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.ParseException;
import org.apache.http.auth.AuthSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
//import org.apache.http.util.EntityUtils;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simba.sqlengine.dsiext.dataengine.Identifier;
import com.simba.support.ILogger;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ErrorException;
import com.useready.rester.DownloadArrowFile;
//import com.sun.org.apache.bcel.internal.generic.RETURN;
import com.useready.rester.QuickJson;
import com.useready.rester.Settings;
import com.useready.rester.exceptions.URMessageKey;



/**
 * A class containing utility methods used by the core classes.
 */
public class URCoreUtils {
	/*
	 * Static variable(s)
	 * ========================================================================= =
	 */

	/**
	 * Constant variable for the symbol that will be used to identify path delimiter
	 * for java.
	 */
	private static final String HOST_DELIMITER = "//";

	/**
	 * Constant variable for the symbol that will be used to identify different key
	 * value pairs in the connection string.
	 */
	private static final String KEY_VALUE_DELIMITER = ";";

	/**
	 * Constant variable used to identify the values in the key value pairs.
	 */
	private static final String VALUE_DELIMITER = "=";

	public static String header = "";
	
	private static CountDownLatch latch;
	
	

	/*
	 * Static Method(s)
	 * ========================================================================= ===
	 */

	/**
	 * Function to check if the table is in the mapping. Will be able to find tables
	 * if it is not given catalog and/or schema.
	 *
	 * @param catalog            The catalog where the table is in. If empty then no
	 *                           catalog is given.
	 * @param schema             The schema where the table is in. If empty then no
	 *                           schema is given.
	 * @param table              The table name. Cannot be empty.
	 * @param mapping            The mapping of catalog to schema to table to check
	 *                           for the table.
	 * @param outTableIdentifier Hold catalog, schema and table name where they have
	 *                           been provided or not.
	 *
	 * @return true if table exists in the mapping. false if table does not exist or
	 *         duplicate table name exists.
	 * 
	 * 
	 */
	public static boolean findTableInMapping(String catalog, String schema, String table,
			Map<String, Map<String, List<String>>> mapping, Identifier outTableIdentifier,ILogger log) {
		// Have the current catalog, schema and table name in the identifier by
		// default.
		outTableIdentifier.setCatalog(catalog);
		outTableIdentifier.setSchema(schema);
		outTableIdentifier.setName(table);

		if (!catalog.isEmpty()) {
			// Catalog is given.
			Map<String, List<String>> currentCatalog = mapping.get(catalog);

			if (null == currentCatalog) {
				// The catalog given does not exist.
				return false;

			}

			// If the catalog exists, go to find the schema and table.
			return findSchemaAndTable(outTableIdentifier, currentCatalog);
		}
		boolean isTableFound = false;

		// Catalog is not given.
		for (String currentCatalogName : mapping.keySet()) {
			// Iterate through all catalogs in the mapping.
			Map<String, List<String>> currentCatalog = mapping.get(currentCatalogName);

			if (null == currentCatalog) {
				// Catalog is not given and no catalog exists in the data
				// source.
				return false;
			}

			if (findSchemaAndTable(outTableIdentifier, currentCatalog)) {
				if (isTableFound) {
					return false;
				}

				isTableFound = true;

				// Set the current catalog name as the new catalog name.
				outTableIdentifier.setCatalog(currentCatalogName);
			}
		}

		return isTableFound;
	}

	/**
	 * Loads all the tables from the given database path that have a catalog and
	 * schema and then stores it in a data structure.
	 *
	 * @param dbfPath The path of the file in the database.
	 * @param iLogger
	 *
	 * @return Mapping of all tables with catalog and schema in the given path
	 * @throws ErrorException
	 * @throws IOException
	 * @throws java.text.ParseException 
	 */
	public static Map<String, Map<String, List<String>>> loadTables(String dbfPath, List<JsonNode> nodeList,
			ILogger iLogger, Settings m_settings) throws ErrorException, IOException, java.text.ParseException {
		Map<String, Map<String, List<String>>> tableMapping = new TreeMap<String, Map<String, List<String>>>();
		for (JsonNode node : nodeList) {
			Iterator<Entry<String, JsonNode>> objectSchema = null;

			ObjectMapper mapper = new ObjectMapper();
			//LogUtilities.logError("--loadTables---rootNode------" + node, iLogger);
			JsonNode rootNode = node;
			//LogUtilities.logError("--loadTables---rootNode------" + rootNode.isObject(), iLogger);
			if (rootNode == null)
				continue;

			if (m_settings.m_format.equals("oneCall")) {
				// Load On Demand
				//LogUtilities.logError("-Root Node is object------" + rootNode.toString(), iLogger);
				tableMapping = readOnlyCatalogs(tableMapping, nodeList, iLogger, m_settings);
				//LogUtilities.logError("--Table Mapping---------" + tableMapping.toString(), iLogger);
				break;
			} else if (rootNode.isArray()) {
				//LogUtilities.logError("-Root Node is Array------" + rootNode.toString(), iLogger);
				if(m_settings.m_format.equals("loadOnDemand")) {
					tableMapping =createCatalogArray(tableMapping,rootNode, iLogger, m_settings);
				}else{
					//LogUtilities.logError("-Root Node is Array else------" + rootNode.toString(), iLogger);
					JsonNode m_currentRowNode = rootNode.get(0);
					if (!m_currentRowNode.isArray()) {
						// one call load all metaData
						tableMapping = readSchema(tableMapping, nodeList);
						break;
					} else {
						Iterator<Entry<String, JsonNode>> objectIterator = m_currentRowNode.fields();
						while (objectIterator.hasNext()) {
							Entry<String, JsonNode> object = objectIterator.next();
							StringBuilder name = new StringBuilder();
							String baseName = null;
							if (null == baseName) {
								name.append(object.getKey());
								String catalog = object.getKey().toString();
								Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(
										String.CASE_INSENSITIVE_ORDER);
								tableMapping.put(catalog, currentCatalog);
								List<String> currentSchema = new ArrayList<String>();

								if (object.getValue() != null && !(0 == object.getValue().size())) {
									JsonNode m_currentSchemaNode = object.getValue();
									Iterator<Entry<String, JsonNode>> schemaIterator = m_currentSchemaNode.fields();
									while (schemaIterator.hasNext()) {
										Entry<String, JsonNode> schema = schemaIterator.next();
										String schemaName = schema.getKey().toString();
										currentCatalog.put(schemaName, currentSchema);
										JsonNode m_currentTabelNode = schema.getValue();
										Iterator<Entry<String, JsonNode>> tabelIterator = m_currentTabelNode.fields();
										while (tabelIterator.hasNext()) {
											Entry<String, JsonNode> tabel = tabelIterator.next();
											String tabelName = tabel.getKey().toString();
											currentSchema.add(tabelName);

										}

										currentSchema = new ArrayList<String>();

									}
								} else {
									currentSchema.add(catalog);

									currentCatalog.put(catalog, currentSchema);
								}

								// break;
							}
						}
					}
				}

				// }

			}
		}
		return tableMapping;
	}

	/**
	 * Parse the subname section of the JDBC URL used to connect to this driver.
	 * Return true if this is an acceptable subname for this driver, otherwise
	 * return false. A full URL might look like
	 * &lt;jdbc&gt;:&lt;subprotocol&gt;:&lt;subname&gt;. The subname section might
	 * contain the DSN name, a port, or other information.
	 *
	 * @param subname    The subname section of the given URL.
	 * @param properties The properties to add the parsed information to.
	 * 
	 * @return true if subname matches for this driver; false otherwise.
	 */
	public static boolean parseSubName(String subname, Properties properties) {
		// At least the host is a required value.
		if ((null == subname) || (0 == subname.length()) || !subname.startsWith(HOST_DELIMITER)) {
			return false;
		}

		// Get rid of the host delimiter.
		String keyValueStr = subname.trim().substring(HOST_DELIMITER.length());

		if (0 == keyValueStr.length()) {
			// Host is a required value.
			return false;
		}

		// Properties are case sensitive so parse the URL values first and then
		// put into
		// the properties at the end.
		TreeMap<String, String> map = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);

		// Place the <key>=<value> pairs in the properties, if any are present.
		String[] keyValuePairs = keyValueStr.split(KEY_VALUE_DELIMITER);

		// Loop through all the pairs and add each <key>=<value> pair.
		for (int i = 0; i < keyValuePairs.length; i++) {
			String[] keyValue = keyValuePairs[i].split(VALUE_DELIMITER);

			if (2 > keyValue.length) {
				// It is legal to have an empty value.
				map.put(keyValue[0], "");
			} else {
				map.put(keyValue[0], keyValue[1]);
			}
		}

		// Search for keys in the case insensitive map and replace the values of
		// the properties if
		// they're presented.
		Enumeration<Object> keys = properties.keys();

		String key;

		while (keys.hasMoreElements()) {
			// Grab the values from the properties that are not in the map.
			key = (String) keys.nextElement();

			if (!map.containsKey(key)) {
				map.put(key, properties.getProperty(key));
			}
		}

		// Replace the set of properties with the new and replaced properties.
		properties.clear();
		properties.putAll(map);

		return true;
	}

	/**
	 * Check if the table is in the specified catalog.
	 * 
	 * @param tableIdentifier Hold catalog, schema and table name where they have
	 *                        been provided or not.
	 * @param currentCatalog  The map of the current catalog.
	 *
	 * @return true if table exists in the current catalog. false if table does not
	 *         exist or or duplicate table name exists.
	 */
	private static boolean findSchemaAndTable(Identifier tableIdentifier, Map<String, List<String>> currentCatalog) {
		if (!tableIdentifier.getSchema().isEmpty()) {
			// Schema is given.
			List<String> currentSchema = currentCatalog.get(tableIdentifier.getSchema());

			if (null == currentSchema) {
				// The schema given does not exist.
				return false;
			}

			// If the schema exists, go to find the table.
			return findTable(tableIdentifier, currentSchema);
		}

		// Initialize that the table has not been found already.
		boolean isTableFound = false;

		// Schema is not given.
		for (String currentSchemaName : currentCatalog.keySet()) {
			// Iterate through all schemas in the current catalog.
			List<String> currentSchema = currentCatalog.get(currentSchemaName);

			if (null == currentSchema) {
				continue;
			}

			if (findTable(tableIdentifier, currentSchema)) {
				if (isTableFound) {
					// The table was already found in another schema, which
					// means the table name
					// is not unique and cannot be resolved to a single table.
					return false;
				}

				isTableFound = true;

				// Set the current schema name as the new schema name.
				tableIdentifier.setSchema(currentSchemaName);
			}
		}

		// return isTableFound;
		return isTableFound;
	}

	/**
	 * Check if the table is in the specified schema.
	 *
	 * @param tableIdentifier Hold catalog, schema and table name where they have
	 *                        been provided or not.
	 * @param currentSchema   The list of the current schema.
	 *
	 * @return true if table exists in the current schema. false if table does not
	 *         exist.
	 */
	private static boolean findTable(Identifier tableIdentifier, List<String> currentSchema) {
		for (String tableName : currentSchema) {
			// Iterate through all tables in the current schema and
			// case-insensitive
			// check the table name.
			if (tableName.equalsIgnoreCase(tableIdentifier.getName())) {
				// Table is found.
				return true;
			}
		}

		// Table is not found.
		return false;
	}

	/**
	 * Get a FilenameFilter that will filter out any files that are not .json.
	 *
	 * @return A FilenameFilter that will only accept .json files.
	 */
	private static FilenameFilter acceptJsonFiles() {
		return new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.toLowerCase().endsWith(QuickJson.QJ_DATAFILE_EXTENSION);
			}
		};
	}

	/**
	 * Get a FileFilter that will filter out any non-directories.
	 *
	 * @return A FileFilter that will only accept directories.
	 */
	private static FileFilter directoriesOnly() {
		return new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.isDirectory();
			}
		};
	}

	public static Map<String, Map<String, List<String>>> readJsonObject(JsonNode rootNode) throws ErrorException {
		Map<String, Map<String, List<String>>> tableMapping = new TreeMap<String, Map<String, List<String>>>(
				String.CASE_INSENSITIVE_ORDER);
		Iterator<Entry<String, JsonNode>> objectSchema = null;
		for (int i = 0; i < rootNode.size(); ++i) {
			JsonNode m_currentRowNode = rootNode.get(i);
			Iterator<JsonNode> m_rootDocIterator = rootNode.elements();

			if (0 == rootNode.size()) {
				// Throw error exception if the root array is empty.
				ErrorException ee = URDriver.s_QJMessages.createGeneralException(URMessageKey.EMPTY_ARRAY.name());
				throw ee;
			}

			Iterator<Entry<String, JsonNode>> objectIterator = m_currentRowNode.fields();
			if (!objectIterator.hasNext()) {
				return tableMapping;
			}
			while (objectIterator.hasNext()) {
				Entry<String, JsonNode> object = objectIterator.next();
				StringBuilder name = new StringBuilder();
				String baseName = null;
				if (null == baseName) {
					// Append the current key name directly when the
					// baseName is null.
					// This happens when the node passed in is a row node.
					name.append(object.getKey());
					String catalog = object.getKey().toString();
					JsonNode schemaNodes = object.getValue();
					Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(
							String.CASE_INSENSITIVE_ORDER);
					objectSchema = schemaNodes.fields();

					while (objectSchema.hasNext()) {
						// Insert currentCatalog to the map while using the
						// catalog as the key value.
						tableMapping.put(catalog, currentCatalog);
						Entry<String, JsonNode> schemaItaor = objectSchema.next();
						List<String> currentSchema = new ArrayList<String>();
						String catalogs = schemaItaor.getKey().toString();
						JsonNode schemaNodesS1 = schemaItaor.getValue();
						Iterator<Entry<String, JsonNode>> schemaS1 = schemaNodesS1.fields();
						while (schemaS1.hasNext()) {
							Entry<String, JsonNode> table = schemaS1.next();
							currentSchema.add(table.getKey().toString());
						}
						currentCatalog.put(catalogs, currentSchema);
					}
				}

			}
		}
		return tableMapping;
	}

	public static Map<String, Map<String, List<String>>> readCatalogsSchema(
			Map<String, Map<String, List<String>>> tableMapping, JsonNode m_currentRowNode) {
		Iterator<Entry<String, JsonNode>> objectIterator = m_currentRowNode.fields();
		Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
		int i = 0;
		String catalog = null;
		String schemaName = null;
		while (objectIterator.hasNext()) {
			Entry<String, JsonNode> object = objectIterator.next();
			StringBuilder name = new StringBuilder();
			List<String> currentSchema = new ArrayList<String>();
			if (i == 0) {
				String baseName = null;
				if (null == baseName) {
					name.append(object.getKey());
					catalog = object.getValue().toString().replace("\"", "");
					i++;
				}
			} else if (i == 1) {
				schemaName = object.getKey().toString();

				i++;
			} else {
				String tabelName = object.getKey().toString();
				currentSchema.add(tabelName);
				currentCatalog.put(schemaName, currentSchema);
			}
		}
		tableMapping.put(catalog, currentCatalog);
		return tableMapping;
	}

	public static Map<String, Map<String, List<String>>> readSchema(Map<String, Map<String, List<String>>> tableMapping,
			List<JsonNode> m_currentRowNode) {
		JsonNode schemaList = m_currentRowNode.get(1);
		JsonNode tablesList = m_currentRowNode.get(2);

		// int i=0;
		String catalog = null;
		boolean sche = false;
		JsonNode m_catalogsNode = m_currentRowNode.get(0);
		int cat = 0;
		Iterator<Entry<String, JsonNode>> dbIterator = m_catalogsNode.fields();
		while (dbIterator.hasNext()) {

			Entry<String, JsonNode> dbobject = dbIterator.next();
			if (cat == 1) {
				JsonNode catalogList = dbobject.getValue();
				for (int ca = 0; ca <= catalogList.size() - 1; ca++) {
					catalog = catalogList.get(ca).toString().replace("\"", "");
					Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(
							String.CASE_INSENSITIVE_ORDER);
					for (int i = 0; i <= schemaList.size() - 1; i++) {

						String schemaName = null;
						// List<String> currentSchema = new ArrayList<String>();
						schemaName = schemaList.get(i).toString().replace("\"", "");
						int c = 0;
						Iterator<Entry<String, JsonNode>> objectIterator = tablesList.fields();
						while (objectIterator.hasNext()) {

							Entry<String, JsonNode> object = objectIterator.next();
							StringBuilder name = new StringBuilder();

							if (c == 1) {
								JsonNode snapShotList = object.getValue();
								List<String> currentSchema = new ArrayList<String>();

								for (int a = 0; a <= snapShotList.size() - 1; a++) {

									JsonNode schemaNode = snapShotList.get(a);
									if (schemaName.equals(schemaNode.get(2).toString().replace("\"", ""))
											&& catalog.equals(schemaNode.get(6).toString().replace("\"", ""))) {

										currentSchema.add(schemaNode.get(1).toString().replace("\"", ""));
									} else {
										currentSchema.addAll(new ArrayList<String>());

									}

								}
								currentCatalog.put(schemaName, currentSchema);

							}
							c++;

						}
					}
					tableMapping.put(catalog, currentCatalog);
				}
			}
			cat++;
		}

		return tableMapping;
	}

	/// Read only Catalogs Names
	public static Map<String, Map<String, List<String>>> readOnlyCatalog(
			Map<String, Map<String, List<String>>> tableMapping, List<JsonNode> m_currentRowNode) {
		JsonNode schemaList = null;
		JsonNode tablesList = null;
		if (m_currentRowNode.size() > 1) {
			m_currentRowNode.get(1);
			if (m_currentRowNode.size() > 2) {
				m_currentRowNode.get(2);
			}
		}

		String catalog = null;
		boolean sche = false;
		JsonNode m_catalogsNode = m_currentRowNode.get(0);
		int cat = 0;
		Iterator<Entry<String, JsonNode>> dbIterator = m_catalogsNode.fields();
		while (dbIterator.hasNext()) {

			Entry<String, JsonNode> dbobject = dbIterator.next();
			if (cat == 1) {
				JsonNode catalogList = dbobject.getValue();

				for (int ca = 0; ca <= catalogList.size() - 1; ca++) {
					catalog = catalogList.get(ca).toString().replace("\"", "");
					// if(catalog.equals("Risk")){

					Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(
							String.CASE_INSENSITIVE_ORDER);
					tableMapping.put(catalog, currentCatalog);
					// }
				}

			}
			cat++;
		}

		return tableMapping;
	}

	public static Map<String, Map<String, List<String>>> readOnlyCatalogs(
			Map<String, Map<String, List<String>>> tableMapping, List<JsonNode> m_currentRowNode, ILogger iLogger,
			Settings m_settings) {
		JsonNode schemaList = null;
		JsonNode tablesList = null;
		if (m_currentRowNode.size() > 1) {
			m_currentRowNode.get(1);
			if (m_currentRowNode.size() > 2) {
				m_currentRowNode.get(2);
			}
		}

		String catalog = null;
		boolean sche = false;
		JsonNode m_catalogsNode = m_currentRowNode.get(0);
		int cat = 0;
	//	Iterator<Entry<String, JsonNode>> dbIterator = m_catalogsNode.fields();
		String metaViews = "";
		//while (dbIterator.hasNext()) {

			//Entry<String, JsonNode> dbobject = dbIterator.next();

			if (cat == 0) {
			//	JsonNode ext_TablesList = null;
				//LogUtilities.logDebug("-- dbobject.getValue().get(0);---------"+ dbobject.getValue(),iLogger);
				// Settings m_settings = new Settings();
				if (m_settings.m_format != null
						&& !(m_settings.m_format.toString().replace("\"", "")).equals("loadOnDemand")) {
				//	ext_TablesList = dbobject.getValue().get(0);
					LogUtilities.logDebug("-- ext_TablesList----"+ m_catalogsNode,iLogger);
					return tableMapping = readTableMap(tableMapping, m_catalogsNode, iLogger);
				} else {
					//ext_TablesList = dbobject.getValue();
				//	if(dbobject.getValue().toString().contains("{")) {
					LogUtilities.logDebug("-m_catalogsNode---"+ m_catalogsNode,iLogger);
						 tableMapping = readTableMapM(tableMapping, m_catalogsNode, iLogger);
					//}
					LogUtilities.logDebug("--tableMapping.get(0);---------"+ tableMapping.toString(),iLogger);
					//ext_TablesList = dbobject.getValue();
				}

//				for (int i = 0; i <= ext_TablesList.size() - 1; i++) {
//					JsonNode node = ext_TablesList.get(i);
//					if (node.has("colName")) {
//
//						if (node.get("colName") != null
//								&& (node.get("colName").toString().replace("\"", "")).equals("MetaViews")) {
//							// LogUtilities.logError("--MetaViews---------"+
//							// node.get("MetaViews").toString(), iLogger);
//							metaViews = node.get("idx").toString().replace("\"", "");
//						}
//
//					}
//				}
			}
			Iterator<Entry<String, JsonNode>> dbIterator = m_catalogsNode.fields();
			while (dbIterator.hasNext()) {
				Entry<String, JsonNode> dbobject = dbIterator.next();
			if (cat == 11) {
				JsonNode catalogList = dbobject.getValue();
				for (int ca = 0; ca <= catalogList.size() - 1; ca++) {
					JsonNode catalogArr = catalogList.get(ca);
					for (int c = 0; c <= catalogArr.size() - 1; c++) {
						if (c == Integer.parseInt(metaViews)) {
							JsonNode cataLogsList = catalogArr.get(c);
							for (int js = 0; js <= cataLogsList.size() - 1; js++) {
								catalog = cataLogsList.get(js).toString().replace("\"", "");
								Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>(
										String.CASE_INSENSITIVE_ORDER);
								tableMapping.put(catalog, currentCatalog);

							}

						}

					}
				}
				LogUtilities.logDebug("--Table Mapping---------" + tableMapping.toString(), iLogger);
			}
	}
			cat++;
		//}

		return tableMapping;
	}

	public static Map<String, Map<String, List<String>>> readTableMap(
			Map<String, Map<String, List<String>>> tableMapping, JsonNode ext_TablesList, ILogger iLogger) {
		JsonNode schemaList = null;
		JsonNode tablesList = null;

		String catalog = null;
		boolean sche = false;
		int cat = 0;
		//Iterator<Entry<String, JsonNode>> dbIterator = ext_TablesList.fields();
		String metaViews = "";
	//	while (dbIterator.hasNext()) {
		
			//Entry<String, JsonNode> dbobject = dbIterator.next();
			///LogUtilities.logDebug("--Table Mapping---MM------" + dbobject.getValue(), iLogger);

//			if (cat == 0) {
//				ext_TablesList = dbobject.getValue();
//
//				for (int i = 0; i <= ext_TablesList.size() - 1; i++) {
//					JsonNode node = ext_TablesList.get(i);
//					if (node.has("dataset")) {
//
//						if (node.get("dataset") != null && (node.get("dataset").toString().replace("\"", "")).equals("string")) {
//						}
//
//					}
//				}
//			}
			if (cat == 0) {
				JsonNode catalogList =ext_TablesList;
				LogUtilities.logDebug("--Table Mapping---MM------" + catalogList, iLogger);
				for (int ca = 0; ca <= catalogList.size() - 1; ca++) {
					// JsonNode catalogArry = catalogList.get(ca);
					JsonNode catalogArr = catalogList.get(ca);
					LogUtilities.logDebug("--Table Mapping---catalogArr------" + catalogArr, iLogger);
					String id = null;
					if (catalogArr.get("dataset") != null) {
						id = catalogArr.get("dataset").toString();
					}
				

					if (id != null && id.contains("/")) {
						String cataLogsList[] = id.split("/");
						String catalogName = null;
						String schemaName = null;
						String tableName = null;
						Map<String, List<String>> currentSchema = new TreeMap<String, List<String>>(
								String.CASE_INSENSITIVE_ORDER);
						List<String> currentTable = new ArrayList<String>();
						for (String str : cataLogsList) {
							if (catalogName == null) {
								catalogName = str.replace("\"", "");
							} else if (catalogName != null && schemaName == null) {
								schemaName = str.replace("\"", "");
							} else {
								String tName=null;
								if (catalogArr.get("viewName") != null) {
									tName = catalogArr.get("viewName").toString();
								}
								
								tableName = tName.replace("\"", "");

							}

						}
						currentTable.add(tableName.replace("\"", ""));
						currentSchema.put(schemaName, currentTable);
						tableMapping.put(catalogName, currentSchema);
					}
				}
				LogUtilities.logDebug("--Table Mapping---------" + tableMapping.toString(), iLogger);
			}
			cat++;
		//}

		return tableMapping;
	}

	
	public static Map<String, Map<String, List<String>>> readTableMapM(
			Map<String, Map<String, List<String>>> tableMapping, JsonNode ext_TablesList, ILogger iLogger) {
		JsonNode schemaList = null;
		JsonNode tablesList = null;

		String catalog = null;
		boolean sche = false;
		int cat = 0;
		Iterator<Entry<String, JsonNode>> dbIterator = ext_TablesList.fields();
		String metaViews = "";
		while (dbIterator.hasNext()) {

			Entry<String, JsonNode> dbobject = dbIterator.next();

			if (cat == 0) {
				ext_TablesList = dbobject.getValue();
				for (int i = 0; i <= ext_TablesList.size() - 1; i++) {
					JsonNode node = ext_TablesList.get(i);
					if (node.has("id")) {

						if (node.get("id") != null && (node.get("id").toString().replace("\"", "")).equals("string")) {
						}

					}
				}
			}
			if (cat == 1) {
				JsonNode catalogList = dbobject.getValue();
				for (int ca = 0; ca <= catalogList.size() - 1; ca++) {
					// JsonNode catalogArry = catalogList.get(ca);
					JsonNode catalogArr = catalogList.get(ca);
					String id = null;
					if (catalogArr.get("id") != null) {
						id = catalogArr.get("id").toString();
					}

					if (id != null && id.contains("/")) {
						String cataLogsList[] = id.split("/");
						String catalogName = null;
						String schemaName = null;
						String tableName = null;
						Map<String, List<String>> currentSchema = new TreeMap<String, List<String>>(
								String.CASE_INSENSITIVE_ORDER);
						List<String> currentTable = new ArrayList<String>();
						for (String str : cataLogsList) {
							if (catalogName == null) {
								catalogName = str.replace("\"", "");
							} else if (catalogName != null && schemaName == null) {
								schemaName = str.replace("\"", "");
							} else {
								tableName = str.replace("\"", "");

							}

						}
						if(tableMapping.containsKey(catalogName)) {
							Map<String, List<String>> schemaLists = tableMapping.get(catalogName);
							if(schemaLists.containsKey(schemaName)) {
								List<String> tablelIst = schemaLists.get(schemaName);
								tablelIst.add(tableName.replace("\"", ""));
								schemaLists.put(schemaName, tablelIst);
								tableMapping.put(catalogName, schemaLists);
							}
						}else{
							currentTable.add(tableName.replace("\"", ""));
							currentSchema.put(schemaName, currentTable);
							tableMapping.put(catalogName, currentSchema);
						}
						
					}
				}
				LogUtilities.logDebug("--Table Mapping---------" + tableMapping.toString(), iLogger);
			}
			cat++;
		}

		return tableMapping;
	}

	public static JsonNode readDatafromJson(String tableUrl, ILogger iLogger, Settings m_settings, String method,
			JsonNode requestBody,boolean redirect) {
		JsonNode rootNode = null;
		boolean test = true;
		LogUtilities.logDebug("Read Data from json ---------- "+m_settings.m_authType + tableUrl, iLogger);
		try {
			if (!test) {
				LogUtilities.logDebug("Url---------- " + tableUrl, iLogger);
				URL urls = new URL(tableUrl);
				HttpURLConnection conn = (HttpURLConnection) urls.openConnection();
//                 conn.setHostnameVerifier(hv);
				conn.setRequestMethod("GET");
				String basicAuth = "Basic " + m_settings.token;
				conn.setRequestProperty ("Authorization", basicAuth);
				conn.connect();
				BufferedReader ins = null;
				ObjectMapper mapper = new ObjectMapper();
				// Getting the response code
				int responsecode = conn.getResponseCode();
//                 String msg = conn.getResponseMessage();
				LogUtilities.logDebug("conn.responsecode()---------- " + responsecode, iLogger);
				if (responsecode != 200) {

					LogUtilities.logError("conn.getResponseMessage()---------- " + conn.getResponseMessage().toString(),
							iLogger);
					throw new RuntimeException("HttpResponseCode: " + responsecode);
				} else {
					LogUtilities.logDebug("conn.getResponseMessage() after header set---------- "
							+ conn.getResponseMessage().toString(), iLogger);
					ins = new BufferedReader(new InputStreamReader(conn.getInputStream()));

					// Compressed stream reader to be utilized.
					String inputLine;
					StringBuffer response = new StringBuffer();
					// String test = "";
					while ((inputLine = ins.readLine()) != null) {

						response.append(inputLine);
					}
					ins.close();

					// TBD: replace with SAX
					LogUtilities.logDebug("Read Data from Response : Json Data---------- " +response.toString(), iLogger);
					rootNode = mapper.readTree(response.toString().replaceAll("\\â€œ", "\""));
					LogUtilities.logDebug("Read Data from JSON : Json Data---------- " + rootNode.toString(), iLogger);

				}

			} else if(m_settings.m_authType!=null && m_settings.m_authType.equalsIgnoreCase("Kerberos")) {
				//rootNode = httpClienturl(tableUrl, iLogger, m_settings, method, requestBody);
				
				rootNode=kerberosTester(tableUrl, iLogger, m_settings, method, requestBody);
			}
			else if(m_settings.m_authType==null || m_settings.m_authType.isEmpty()) {
				rootNode=tester(tableUrl, iLogger, m_settings, method, requestBody);
				LogUtilities.logDebug("tester-- "+rootNode.toString(), iLogger);
			}
//			else if(m_settings.m_authType!=null && m_settings.m_authType.equalsIgnoreCase("oauths")) {
//				LogUtilities.logDebug("tester  Arrrow-- "+tableUrl, iLogger);
//				DownloadArrowFile downloadArrowFile =new DownloadArrowFile(tableUrl,iLogger);
//				LogUtilities.logDebug("tester-- "+rootNode.toString(), iLogger);
//			}
			else if(m_settings.m_authType!=null && m_settings.m_authType.equalsIgnoreCase("oauth")) {
				JsonNode node;
		    	ObjectMapper mapper = new ObjectMapper();
				LogUtilities.logDebug("oauthConnnection-- "+tableUrl, iLogger);
				try {
					CloseableHttpClient httpClient = HttpClients.createDefault() ;
		    	     HttpGet httpGet = new HttpGet(tableUrl);
		    	     httpGet.addHeader("Authorization", "Bearer " +m_settings.token);
		    	    // connection.setRequestProperty("User-Agent", "Java GET Request");

//		    	     }
		    	    try {
		    	    	CloseableHttpResponse response = httpClient.execute(httpGet) ;
		    	    	
		    	    	// LogUtilities.logDebug("Response -Body------- "+response.getEntity(), iLogger);
		    	        String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
		    	        if(redirect) {
//		    	        	 node = mapper.readTree(responseBody.trim());
//		    	        	 if (node.has("redirectUrl")) {
//									m_settings.m_tables = node.get("redirectUrl").toString()
//											.replace("\"", "");
//								}

		    	        	m_settings.m_tables=responseBody.trim().replace("\"", "");
		    	        }else {
		    	        	 LogUtilities.logDebug("Response -------- "+responseBody.trim(), iLogger);
				    	        node = mapper.readTree(responseBody.trim());
				    	        if(node.has("body")) {
				    	        	JsonNode node2=node.get("body");
				    	        	  String jsonStringOutput = node2.toString();
				    	        	  
				    	        	 LogUtilities.logDebug("Response Body-- "+jsonStringOutput, iLogger);
				    	        	 
				    	        	 ObjectMapper mapp = new ObjectMapper();
					    	        JsonNode node1 = mapp.readTree(jsonStringOutput);
					    	        rootNode=node1;
					    	        LogUtilities.logDebug("JsonNode2-- "+rootNode.toString(), iLogger);
					    	       // return node2;
				    	      }else {
				    	    	  rootNode = mapper.readTree(responseBody);  
					    	        LogUtilities.logDebug("JsonNode-- "+node.toString(), iLogger);
				    	      }
		    	        }
		    	       
		    	        
		    	        
		    	    } catch (Exception e) {
		    	    	LogUtilities.logError("Exception-- "+e.getMessage(), iLogger);
					}
			}catch (Exception e) {
				LogUtilities.logError("Exception-- "+e.getMessage(), iLogger);
			}
			}
		}catch (Exception e) {
			LogUtilities.logError("Exception-- "+e.getMessage(), iLogger);
		}
	return rootNode;

	}

	private static String getConfigPath() {
		String jarPath = URCoreUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath(); // .toURI

		if (jarPath.endsWith(".jar"))
			jarPath = jarPath.substring(0, jarPath.lastIndexOf("/"));
		// }
		ObjectMapper mapper = new ObjectMapper();

		if (jarPath.substring(0, 1).contains("/")) {
			jarPath = jarPath.substring(1, jarPath.length());
		}
		jarPath = jarPath.replace("/", "\\");
		jarPath = jarPath.replace("%20", " ");
		File directory = new File("");
		jarPath = jarPath + "\\login.conf";
		directory = new File(jarPath);
		return jarPath;
	}
	
	


	public static final String SPNEGO_OID = "1.3.6.1.5.5.2";

	private static final String KERBEROS_OID = "1.2.840.113554.1.2.2";

	
	 private static Comparator<String> getDateStringComparator(String order) {
	        return new Comparator<String>() {
	            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	            
	            @Override
	            public int compare(String dateString1, String dateString2) {
	                try {
	                    // Parse the dates as strings into Date objects
	                    Date date1 = dateFormat.parse(dateString1);
	                    Date date2 = dateFormat.parse(dateString2);
	                    // Compare the dates
	                    if(order!=null && order.equalsIgnoreCase("asc")) {
	                    	 return date1.compareTo(date2);	
	                    }else if(order!=null && order.equalsIgnoreCase("desc")) {
	                    	 return date2.compareTo(date1);
	                    }
	                   
	                } catch (ParseException | java.text.ParseException e) {
	                    e.printStackTrace();
	                }
	                return dateString1.compareTo(dateString2);
	            }
	        };
	    }
	 private static Map<String, Map<String, List<String>>> createCatalogArray(Map<String, Map<String, List<String>>> tableMapping,JsonNode node, ILogger iLogger, Settings m_settings) throws java.text.ParseException {
			Map<String, List<String>> currentCatalog = new TreeMap<String, List<String>>();
			List<String> currentSchema = new ArrayList<String>();
			   List<Date> dateList = new ArrayList<>();
			//   List<String> stringList = new ArrayList<>();
		       // SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		        if(m_settings.m_databaseOrder!=null && m_settings.m_databaseOrder!="" && !m_settings.m_databaseOrder.isEmpty()) {
		        	//LogUtilities.logError("COB Date m_settings.m_databaseOrder-- " +m_settings.m_databaseOrder, iLogger);
		        	tableMapping  = new TreeMap<>(getDateStringComparator(m_settings.m_databaseOrder));
		        }
		       
			for (int i = 0; i <= node.size() - 1; i++) {
			//	stringList.add((node.get(i).toString().replace("\"", "")));
				tableMapping.put((node.get(i).toString().replace("\"", "")), currentCatalog);
				//LogUtilities.logError("COB Date 1-- " +tableMapping.toString(), iLogger);
			}

			// LogUtilities.logError("COB Date After-- " +tableMapping.toString(), iLogger);
			return tableMapping;
		}

		public static  JsonNode kerberosTester(String url, ILogger iLogger, Settings m_settings,String method,
				JsonNode requestBody){
			LogUtilities.logDebug("Url---------- " +url, iLogger);
			JsonNode node = null;
			HttpEntity entity=null;
			 String krbHost=m_settings.m_host;
			 ObjectMapper mapper = new ObjectMapper();
			System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
			
	  // 	System.setProperty("java.security.auth.login.config", "C:\\Program Files\\Tableau\\Drivers\\login.conf");
		//	System.setProperty("java.security.krb5.conf", "C:\\Windows\\krb5.conf");
//			
//					    if (null != System.getenv("KRB5_CONFIG"))
//			
//					    {
//					        System.setProperty("java.security.krb5.conf", "C:\\Windows\\krb5.conf");
//			
//					    }
	     // LoginContext lc;
		try {
			// lc = new LoginContext("com.sun.security.jgss.krb5.initiate");
			// LogUtilities.logError("Login ----- "+lc.getSubject(), iLogger);		
	       // lc.login();
			LogUtilities.logDebug("Before Subject -- ----- "+m_settings.subject, iLogger);
			Subject sub = null ;
			if(m_settings.subject==null) {
				 sub = Subject.getSubject(AccessController.getContext());
				 m_settings.subject=sub;
				
				 LogUtilities.logDebug("Subject  m_settings.subject----- "+ m_settings.subject, iLogger);
				 }else{
					sub= m_settings.subject;	
					 Set<Principal> principals = sub.getPrincipals();
		               for (Principal next : principals) {
		            	   LogUtilities.logDebug("Subject Principal: " + next.getName(), iLogger);
		               }
					 LogUtilities.logDebug("Subject  m_settings.subject esle----- "+ sub, iLogger); 
				 }
			 
	        
	        CloseableHttpClient httpclient = HttpClientBuilder.create()
	 	        		.addInterceptorLast(new KerberosHttpInterceptor(
	 	        				krbHost,
	 	        				 sub,iLogger))
	 	    			.build();
	        
	        
	        LogUtilities.logDebug("Method Type ----- "+method, iLogger);
	        CloseableHttpResponse response = null ;
	        if (method != null && method.equalsIgnoreCase("POST")) {
        		HttpPost httpPost = new HttpPost(url);
        		httpPost.setHeader("Content-Type", "application/json");
        		httpPost.setEntity(new ByteArrayEntity(requestBody.toString().getBytes("UTF-8")));
        		 response = httpclient.execute(httpPost);
 		    	// LogUtilities.logDebug("response Post-------" + response.getStatusLine(), iLogger);
	        }else{
	        	HttpGet httpGet=new HttpGet(url);
		       response = httpclient.execute(httpGet);
	        }
	    	
	    	 entity = response.getEntity();
	         LogUtilities.logDebug("response ----- ", iLogger);
	         LogUtilities.logDebug("response.getStatusLine() ----- " + response.getStatusLine(), iLogger);
	         if (entity != null)
	           try {
	             String responseString = EntityUtils.toString(entity, "UTF-8");
	             node = mapper.readTree(responseString);
	             LogUtilities.logDebug("response ----- " + responseString, iLogger);
	             LogUtilities.logDebug("response Status ----- " + response.getStatusLine().toString(), iLogger);
	           } catch (Exception e) {
	        	   LogUtilities.logError("printStackTrace ----- " + e.getMessage(), iLogger);
	               e.printStackTrace();
			} 
	         finally {
	        	 try {
		              EntityUtils.consume(entity);
		            } catch (IOException e) {
		              e.printStackTrace();
		            } 
		            httpclient.getConnectionManager().shutdown();
	         	response.close();
	         	httpclient.close();
	              }
	        
		}catch (Exception e) {
			LogUtilities.logError("Exception----- "+e.getMessage(), iLogger);
			// TODO: handle exception
			e.printStackTrace();
		}
		return node;
		}
		public static  JsonNode tester(String url, ILogger iLogger, Settings m_settings,String method,
				JsonNode requestBody){
			LogUtilities.logDebug("Url---------- " +url, iLogger);
			JsonNode node = null;
			HttpEntity entity=null;
			 ObjectMapper mapper = new ObjectMapper();

		try {
			
			//LogUtilities.logDebug("Before Subject -- ----- "+m_settings.subject, iLogger);
			//Subject sub = null ;
	        CloseableHttpClient httpclient = HttpClientBuilder.create().build();
	        
	        LogUtilities.logDebug("Method Type ----- "+method, iLogger);
	        CloseableHttpResponse response = null ;
	        if (method != null && method.equalsIgnoreCase("POST")) {
        		HttpPost httpPost = new HttpPost(url);
        		httpPost.setHeader("Content-Type", "application/json");
        		httpPost.setEntity(new ByteArrayEntity(requestBody.toString().getBytes("UTF-8")));
        		 response = httpclient.execute(httpPost);
 		    	 LogUtilities.logDebug("response Post-------" + response.getStatusLine(), iLogger);
	        }else{
	        	HttpGet httpGet=new HttpGet(url);
		       response = httpclient.execute(httpGet);
	        }
	    	
	    	 entity = response.getEntity();
	         LogUtilities.logDebug("response ----- ", iLogger);
	         LogUtilities.logDebug("response.getStatusLine() ----- " + response.getStatusLine(), iLogger);
	         if (entity != null)
	           try {
	             String responseString = EntityUtils.toString(entity, "UTF-8");
	             node = mapper.readTree(responseString);
	           //  LogUtilities.logError("response ----- " + responseString, iLogger);
	             LogUtilities.logDebug("response Status ----- " + response.getStatusLine().toString(), iLogger);
	           } catch (Exception e) {
	        	   LogUtilities.logError("printStackTrace ----- " + e.getMessage(), iLogger);
	               e.printStackTrace();
			} 
	         finally {
	        	 try {
		              EntityUtils.consume(entity);
		            } catch (IOException e) {
		              e.printStackTrace();
		            } 
		            httpclient.getConnectionManager().shutdown();
	         	response.close();
	         	httpclient.close();
	              }
	        
		}catch (Exception e) {
			LogUtilities.logError("Exception----- "+e.getMessage(), iLogger);
			// TODO: handle exception
			e.printStackTrace();
		}
		return node;
		}
		public static String readOnlyCatalogs(JsonNode m_currentRowNode, ILogger iLogger,
				Settings m_settings) {
			JsonNode schemaList = null;
			JsonNode tablesList = null;
			String url = null;
			String catalog = null;
		String	urlId;
			boolean sche = false;
			JsonNode m_catalogsNode = m_currentRowNode.get(0);
			int cat = 0;
			Iterator<Entry<String, JsonNode>> dbIterator = m_catalogsNode.fields();
			String metaViews = "";
			while (dbIterator.hasNext()) {

				Entry<String, JsonNode> dbobject = dbIterator.next();

				if (cat == 1) {
					JsonNode ext_TablesList = null;
//					if (dbobject.getValue().isArray() && m_settings.m_format != null
//							&& (m_settings.m_format.toString().replace("\"", "")).equals("loadOnDemand")) {
//						ext_TablesList = dbobject.getValue();
//						LogUtilities.logError("--ext_TablesList------"+ ext_TablesList.fields(),iLogger);
//						int c = 0;
//						Iterator<Entry<String, JsonNode>> dbIteratorc = ext_TablesList.fields();
//						while (dbIteratorc.hasNext()) {
//
//							Entry<String, JsonNode> dbobjectc = dbIteratorc.next();
//							LogUtilities.logError("--ext_TablesList-1-----"+dbobjectc.getValue().toString(),iLogger);
//							if (c == 0) {
//								ext_TablesList = dbobjectc.getValue();
//
//								for (int i = 0; i <= ext_TablesList.size() - 1; i++) {
//									JsonNode node = ext_TablesList.get(i);
//									LogUtilities.logError("--ext_TablesList-1--node---"+dbobjectc.getValue().toString(),iLogger);
//									if (node.has("id")) {
//
//										if (node.get("id") != null && (node.get("id").toString().replace("\"", "")).equals("5")) {
//											urlId =node.get("id") .toString();
//											LogUtilities.logError("-- Get 5---------"+ dbobject.getValue(),iLogger);
//										}
//
//									}
//								}
//							}
//					}
//					}
					//cat++;
				}
//				
				if (cat == 2) {
					JsonNode catalogList = dbobject.getValue();
					LogUtilities.logDebug("--catalogList---------" + catalogList.toString(), iLogger);
					for (int ca = 0; ca <= catalogList.size() - 1; ca++) {
						JsonNode catalogArr = catalogList.get(ca);
						url= catalogArr.get(5).toString().replace("\"", "");
						break;
					}
				}
				cat++;
			}
			return url;
		}

}
