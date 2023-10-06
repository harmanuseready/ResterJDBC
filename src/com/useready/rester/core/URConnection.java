// =================================================================================================
///  @file QJConnection.java
///
///  Definition of the Class QJConnection
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simba.dsi.core.impl.DSIConnection;
import com.simba.dsi.core.impl.DSILogger;
import com.simba.dsi.core.interfaces.IStatement;
import com.simba.dsi.core.utilities.ConnPropertyKey;
import com.simba.dsi.core.utilities.ConnPropertyValues;
import com.simba.dsi.core.utilities.ConnSettingRequestMap;
import com.simba.dsi.core.utilities.ConnSettingResponseMap;
import com.simba.dsi.core.utilities.PropertyUtilities;
import com.simba.dsi.core.utilities.Variant;
import com.simba.dsi.exceptions.BadAuthException;
import com.simba.dsi.exceptions.IncorrectTypeException;
import com.simba.dsi.exceptions.NumericOverflowException;
import com.simba.support.ILogger;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ErrorException;
import com.useready.rester.Settings;
import com.useready.rester.exceptions.URMessageKey;
import com.useready.rester.core.jdbc42.*;

/**
 * Implementation of IConnection. Extends DSIConnection which initializes and
 * manages connection properties.
 */
public class URConnection extends DSIConnection {
    /*
     * Static variable(s)
     * ========================================================================= =
     */

    /**
     * A counter for creating unique connection IDs.
     */
    private static AtomicInteger s_connectionID = new AtomicInteger(0);

    /*
     * Instance variable(s)
     * ========================================================================
     */

    /**
     * Connection logger.
     */
    protected ILogger m_log;

    /**
     * Setting to be used for getting the DBF path and to store table lookup
     * information
     */
    public Settings m_settings = new Settings();

    /*
     * Constructor(s)
     * =========================================================================
     * =====
     */

    /**
     * Constructor.
     *
     * @param environment The parent environment.
     *
     * @throws ErrorException If an error occurs.
     */
    public URConnection(UREnvironment environment) throws ErrorException {
        super(environment);
        LogUtilities.logFunctionEntrance(getConnectionLog(), environment);
        setDefaultProperties();
    }

    /*
     * Method(s)
     * =========================================================================
     * ==========
     */

    /**
     * Closes the connection and releases any resources held by it.
     */
    public void close() {
        LogUtilities.logFunctionEntrance(getConnectionLog());
    }

    /**
     * Attempts to establish connection when requested to form a connection, not
     * when the IConnection is allocated. The IConnection can be re-used after
     * disconnect(), therefore calling connect() after disconnect() will set up a
     * new connection on the same IConnection.
     *
     * @param requestMap Connection settings used to authenticate if connection
     *                   should be granted.
     *
     * @throws ErrorException   If an unspecified error occurs.
     * @throws BadAuthException If the inputs fail authentication.
     */
    public void connect(ConnSettingRequestMap requestMap) throws ErrorException, BadAuthException {
        LogUtilities.logFunctionEntrance(getConnectionLog(), requestMap);
   System.out.println("--------------------");
        // TODO #7: Establish A Connection.

        // Retrieve the connection settings of the database.
        // Variant dbPath=getRequiredSetting(QJPropertyKey.SERVER, requestMap);
       LogUtilities.logDebug("java.security.auth.login.config------------ " +System.getProperty("java.security.auth.login.config"),  getConnectionLog());
		
		
     //   LogUtilities.logDebug("--requestMap---------" + requestMap, getConnectionLog());
        Variant server = getRequiredSetting(URPropertyKey.SERVER, requestMap);
        if (requestMap.containsKey(URPropertyKey.USER)) {
            Variant user = getRequiredSetting(URPropertyKey.USER, requestMap);
            m_settings.m_username = user.getString();
//            LogUtilities.logDebug("--USER---------" + user.getString(), getConnectionLog());
//            LogUtilities.logDebug("--USER- m_username--------" + m_settings.m_username, getConnectionLog());
        }
        if (requestMap.containsKey(URPropertyKey.AUTH)) {
            Variant auth = getRequiredSetting(URPropertyKey.AUTH, requestMap);
//            LogUtilities.logDebug("--AUTH---------" + auth.getString(), getConnectionLog());
        }
        if (requestMap.containsKey(URPropertyKey.TOKEN)) {
            Variant token = getRequiredSetting(URPropertyKey.TOKEN, requestMap);
            m_settings.token=token.getString();
//            LogUtilities.logDebug("--TOKEN---------" + m_settings.token, getConnectionLog());
        }
        if (requestMap.containsKey(URPropertyKey.SERVER)) {

            LogUtilities.logDebug("--SERVER---------" + server.getString(), getConnectionLog());
        }
        Variant config = null;
        if (requestMap.containsKey(URPropertyKey.CONFIG)) {

            config = getOptionalSetting(URPropertyKey.CONFIG, requestMap);

            m_settings.m_config = config.getString();
        }
        m_settings.m_server = server.getString();
     //   https://datazone-dznode-https-lite-dev.ms.com/dz/collibra/getEntitledDatasetsDetails
        
       // HardCoded base uRL and Database 
//        m_settings.m_baseUrl="https://datazone-dznode-https-lite-dev.ms.com";
//        m_settings.m_database="/dz/collibra/getEntitledDatasetsDetails";
//        LogUtilities.logDebug("Database URL-----------------" + m_settings.m_baseUrl+m_settings.m_database,
//                getConnectionLog());
        ObjectMapper mapper = new ObjectMapper();
        LogUtilities.logDebug("-Picking json file from Full path------" + m_settings.m_config,
                getConnectionLog());
       if(isFilePath(m_settings.m_config)) {
    	   LogUtilities.logDebug("-Full path------" + m_settings.m_config,
                   getConnectionLog());
         File  directory = new File(m_settings.m_config);
           if (directory != null) {
               StringBuffer sb = new StringBuffer();

               try {

                   Scanner sc = new Scanner(directory);
                   String input;
                   while (sc.hasNextLine()) {
                       input = sc.nextLine();
                       sb.append(input + " ");
                   }
                   JsonNode rootNode = mapper.readTree(sb.toString());
                   if (rootNode.isObject()) {
                       Iterator<Entry<String, JsonNode>> dbIterator = rootNode.fields();
                       while (dbIterator.hasNext()) {

                           Entry<String, JsonNode> dbobject = dbIterator.next();
                           if (dbobject.getKey().toString().equals(m_settings.m_server)) {
                               m_settings.settingsJson(dbobject.getValue(), getConnectionLog());
//                               LogUtilities.logDebug("--m_stttings---------" + m_settings.m_database,
//                                       getConnectionLog());
                           }

                       }

                   }

               } catch (IOException e) {
                   // TODO Auto-generated catch block
                   e.printStackTrace();
               }
           }
       }else {
    	   String jarPath = URConnection.class
                   .getProtectionDomain()
                   .getCodeSource()
                   .getLocation().getPath(); // .toURI
    	   LogUtilities.logDebug("-Picking json file from Default path::" + jarPath,
                   getConnectionLog());
           if (jarPath.endsWith(".jar"))
               jarPath = jarPath.substring(0, jarPath.lastIndexOf("/"));
           // }
          

           if (jarPath.substring(0, 1).contains("/")) {
               jarPath = jarPath.substring(1, jarPath.length());
           }
           jarPath = jarPath.replace("/", "\\");
           jarPath = jarPath.replace("%20", " ");
           File dir = new File("");
           if (config != null && config.getString() != null && config.getString().endsWith(".json")) {
               jarPath = jarPath + "\\" + config.getString();
               dir = new File(jarPath);
               if (dir != null) {
                   StringBuffer sb = new StringBuffer();

                   try {

                       Scanner sc = new Scanner(dir);
                       String input;
                       while (sc.hasNextLine()) {
                           input = sc.nextLine();
                           sb.append(input + " ");
                       }
                       JsonNode rootNode = mapper.readTree(sb.toString());
                       if (rootNode.isObject()) {
                           Iterator<Entry<String, JsonNode>> dbIterator = rootNode.fields();
                           while (dbIterator.hasNext()) {

                               Entry<String, JsonNode> dbobject = dbIterator.next();
                               if (dbobject.getKey().toString().equals(m_settings.m_server)) {
                                   m_settings.settingsJson(dbobject.getValue(), getConnectionLog());
                                   LogUtilities.logDebug("--Database---------" + m_settings.m_database,
                                           getConnectionLog());
                                   LogUtilities.logDebug("--Base url---" + m_settings.m_baseUrlredriect,
                                           getConnectionLog());
                               }

                           }

                       }

                   } catch (Exception e) {
                       // TODO Auto-generated catch block
                	   LogUtilities.logError("Exception e----" +e.getMessage().toString(),
                               getConnectionLog());
                       e.printStackTrace();
                   }
               } else {

               }
           } else {
              File directory = new File(jarPath);
               File[] listOfFiles = directory.listFiles();

               for (int i = 0; i < listOfFiles.length; i++) {
                   File file = listOfFiles[i];
                   if (file.getName().endsWith(".json")) {
                       // String content = FileUtils.readFileToString(file);
                       /* do something with content */
                       StringBuffer sb = new StringBuffer();

                       try {

                           Scanner sc = new Scanner(file);
                           String input;
                           while (sc.hasNextLine()) {
                               input = sc.nextLine();
                               sb.append(input + " ");
                           }
                           JsonNode rootNode = mapper.readTree(sb.toString());
                           if (rootNode.isObject()) {
                               Iterator<Entry<String, JsonNode>> dbIterator = rootNode.fields();
                               while (dbIterator.hasNext()) {

                                   Entry<String, JsonNode> dbobject = dbIterator.next();
                                   if (dbobject.getKey().toString().equals(m_settings.m_server)) {
                                       m_settings.settingsJson(dbobject.getValue(), getConnectionLog());
                                       LogUtilities.logDebug("--m_stttings---------" + m_settings.m_database,
                                               getConnectionLog());
                                       LogUtilities.logDebug("--Base url---" + m_settings.m_baseUrlredriect,
                                               getConnectionLog());
                                   }

                               }

                           }

                       } catch (Exception e) {
                           // TODO Auto-generated catch block
                    	   LogUtilities.logError("Exception e----" +e.getMessage().toString(),
                                   getConnectionLog());
                           e.printStackTrace();
                       }

                   }
               }
           }

       }

        
    

    
        
        // convert JSON file to map
        try {
            // TODO: See if we can read zip/compressed data loading
        	 LogUtilities.logDebug("--Database m_baseUrlredriect--------- " +m_settings.m_baseUrlredriect+"----followHTTPRedirect--------"+m_settings.followHTTPRedirect, getConnectionLog());
        	if(m_settings.m_baseUrlredriect!=null && m_settings.followHTTPRedirect ) {
        		
        		try{
//        			URLConnection con = new URL(m_settings.m_baseUrlredriect).openConnection();
////        			 LogUtilities.logDebug("-Connect before----- " +m_settings.m_baseUrlredriect+"------------"+m_settings.followHTTPRedirect, getConnectionLog());
//                 	
//            	con.connect();
//            	
//            	InputStream is = con.getInputStream();
////            	 LogUtilities.logDebug("-Connect After----- " +is.toString(), getConnectionLog());
//               	
//            	String str=con.getURL().toString().replace("\"", "");
        			String redriecturl =m_settings.m_baseUrlredriect;
        			LogUtilities.logDebug("--Before Redirect url--------- " +redriecturl, getConnectionLog());
        	    	HttpURLConnection con = (HttpURLConnection) new URL(redriecturl).openConnection();
        	    	con.setInstanceFollowRedirects(false);
        	    	con.connect();
        	    	String str = con.getHeaderField("Location").toString(); 
            	 LogUtilities.logDebug("--After Redirect url--------- " +str, getConnectionLog());
            	if(str.contains("\\?")) {
            		//str= str.substring(0,str.lastIndexOf("\\?"));
            		String str1= str.substring(0,str.lastIndexOf("?"));
            		m_settings.m_baseUrl=str1;
            		
            		  LogUtilities.logDebug("--Database Base URL updated--------- " + m_settings.m_baseUrl, getConnectionLog());
            	}else{
            		m_settings.m_baseUrl=str.replace("\"", "");
            		  LogUtilities.logDebug("--Database URl---else------ " + m_settings.m_baseUrl, getConnectionLog());
            	}
            	m_settings.followHTTPRedirect=false;
            	System.out.println( "redirected url: " + con.getURL() );
            	 LogUtilities.logDebug("--Redirected url------ " + m_settings.m_baseUrl, getConnectionLog());
             	
            	//is.close();
        		}catch(Exception e) {
        			LogUtilities.logError("--Exception-- " +e.getMessage(), getConnectionLog());
        			e.printStackTrace();
        		}
//        		JsonNode rootNode;
//                try {
//                    LogUtilities.logDebug("--readDatafromJson--------- " + m_settings.m_baseUrlredriect, getConnectionLog());
//                    rootNode = URCoreUtils.tester(m_settings.m_baseUrlredriect, getConnectionLog(), m_settings, null, null);
//                   String str=URCoreUtils.readOnlyCatalogs(rootNode, getConnectionLog(), m_settings);
//                 
//                   m_settings.m_baseUrl= str;
//                   LogUtilities.logDebug("Base url------ " +  m_settings.m_baseUrl, getConnectionLog());
//                    
//                } catch (Exception e) {
//                    // TODO Auto-generated catch block
//                    LogUtilities.logError("--directory---EX------ " + e.getMessage(), getConnectionLog());
//                    e.printStackTrace();
//                }
//        		
        	}else{
             	
        		m_settings.m_baseUrl=m_settings.m_baseUrlredriect;
        		 LogUtilities.logDebug("--Database m_baseUrl-------- " + m_settings.m_baseUrl, getConnectionLog());
              	
        	}
        	 
        	
            try {
            	 URL url1=new URL(m_settings.m_baseUrl);  
            	 m_settings.m_host=url1.getHost();
            	 LogUtilities.logDebug("-Host-- " +  m_settings.m_host, getConnectionLog());
                String urls = m_settings.m_baseUrl + m_settings.m_database;
                LogUtilities.logDebug("--Database URL--------- " + urls, getConnectionLog());
                JsonNode rootNode;
              //  LogUtilities.logDebug("--readDatafromJson--------- " + urls, getConnectionLog());
                rootNode = URCoreUtils.readDatafromJson(urls, getConnectionLog(), m_settings, null, null,false);
               
                	m_settings.m_rootNode = rootNode;
                    m_settings.m_rootNodeList.add(rootNode);
                
                
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LogUtilities.logError("--Database Exception----- " + e.getMessage(), getConnectionLog());
                e.printStackTrace();
            }

            m_settings.m_tableMap = URCoreUtils.loadTables("", m_settings.m_rootNodeList, getConnectionLog(),
                    m_settings);
            LogUtilities.logDebug("--Database m_settings.m_tableMap----- " +m_settings.m_tableMap, getConnectionLog());
          //  if (m_settings.m_format.equals("loadOnDemands")) {
//                String schemaUrl = m_settings.m_baseUrl + m_settings.m_schema;
//                m_settings.̥ = URCoreUtils.readDatafromJson(schemaUrl, getConnectionLog(), m_settings,null,null);
//               m_settings.m_schemalist=true;
//                JsonNode tablesList = m_settings.m_tablesArr;
//                for (int i = 0; i <= tablesList.size() - 1; i++) {
//                    JsonNode node = tablesList.get(i);
//                    if (node.has("url")) {
//                        if (node.get("url").toString() != null) {
//                            String url = node.get("url").toString().replace("\"", "");
//                            String tablePre = node.get("TablePrefix").toString().replace("\"", "");
//                            String type = node.get("type").toString().replace("\"", "");
//                            String fieldName = node.get("fieldName").toString().replace("\"", "");
//                            if (node.get("displayName").toString() != null && type.isEmpty()) {
//                                String displayName = node.get("displayName").toString().replace("\"", "");
//                                LogUtilities.logDebug("--AddDataIntoArray--------" + url.toString(), getConnectionLog());
//                                Map<String, Map<String, List<String>>> tableMap = new TreeMap<String, Map<String, List<String>>>();
//                                Map<String, Map<String, List<String>>> tableMap2 = new TreeMap<String, Map<String, List<String>>>();
//                                // for(String tableName:str) {
//                                String tableUrl = m_settings.m_baseUrl + url;
//                                JsonNode rootTable = URCoreUtils.readDatafromJson(tableUrl,
//                                        getConnectionLog(), m_settings,null,null);
//                                m_settings.m_Tables.put(url, rootTable);
//                                m_settings.m_tableList=true;
//
//                                // }
//
//                            }}}}
//                
//                for (int i = 0; i <= m_settings.m_fieldsArr.size() - 1; i++) {
//                    JsonNode nodeArr = m_settings.m_fieldsArr.get(i);
//                    if (nodeArr.has("url")) {
//                        if (nodeArr.get("url").toString() != null) {
//                            String url = nodeArr.get("url").toString().replace("\"", "");
//                            String type = nodeArr.get("table").toString().replace("\"", "");
//                            if (!url.isEmpty() && url != null) {
//                                // for (String dbpath : urlList) {
//                                String dbpath = m_settings.m_baseUrl + url;
//
//                                LogUtilities.logDebug("---Modified URLS------" + dbpath + "-", getConnectionLog());
//                                JsonNode rootNodess = URCoreUtils.readDatafromJson(dbpath, getConnectionLog(),
//                                        m_settings,null,null);
//                                m_settings.m_TablesData.put(url, rootNodess);
//                                m_settings.m_tableData=true;
//                            }
//                        }
//                    }
//                }
//               
            	


     //       } else if (m_settings.m_format.equals("oneCall")) {
//                String urlsList = m_settings.m_fields.replaceAll("\\[", "").replaceAll("\\]", "");
//                urlsList = urlsList.replace("\"", "");
//                String urlList[] = urlsList.split(",");
//                for (String dbpath : urlList) {
//                    dbpath = m_settings.m_baseUrl + dbpath;
//                    JsonNode rootNodes = URCoreUtils.readDatafromJson(dbpath, getConnectionLog(), m_settings, null, null);
//                    m_settings.m_rootNodeTableList.add(rootNodes);
//                    // }
//                }

           // }
            LogUtilities.logDebug("--m_tableMap--------" + m_settings.m_tableMap.toString(), getConnectionLog());

        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        // Retrieve the connection settings of the default maximal column size.
        Variant defaultMaxColumnSize = getOptionalSetting(URPropertyKey.DEFAULTMAXCOLUMNSIZE, requestMap);

        try {
            if (null != defaultMaxColumnSize) {
                // The default max column size defined in the connection string
                // should be an
                // integer between 0 and 2147483647.
                m_settings.m_maxColumnSize = defaultMaxColumnSize.getInt();
            }
        } catch (NumericOverflowException e) {
            ErrorException error = URDriver.s_QJMessages.createGeneralException(URMessageKey.NUMERIC_OVERFLOW.name(),
                    e);
            LogUtilities.logError(error, m_log);
            throw error;
        } catch (IncorrectTypeException e) {
            ErrorException error = URDriver.s_QJMessages.createGeneralException(URMessageKey.INCORRECT_TYPE.name(), e);
            LogUtilities.logError(error, m_log);
            throw error;
        }

        // Read a file from the database path.
        // File db = new File(dbPath.getString());

    }

    /**
     * Factory method for creating IStatements.
     *
     * @return IStatement The generated statement.
     *
     * @throws ErrorException If an error occurs.
     */
    public IStatement createStatement() throws ErrorException {
        LogUtilities.logFunctionEntrance(getConnectionLog());

        return new URStatement(this);
    }

    /**
     * Closes the connection. This is not equivalent to destroying the IConnection
     * object. The connection object can be re-used.
     *
     */
    public void disconnect() {
        LogUtilities.logFunctionEntrance(getConnectionLog());
    }

    /**
     * Gets the connection ILogger logging interface.
     *
     * @return Log for connection logging.
     * @see ILogger
     */
    public ILogger getConnectionLog() {
        if (null == m_log) {
            // TODO #5: Set the connection-wide logging details.
            m_log = new DSILogger(URDriver.DRIVER_NAME + "_conn" + s_connectionID.getAndIncrement());
            m_log.setLocale(getLocale());
        }

        return m_log;
    }

    /**
     * Checks and updates settings for this connection. This method is called when
     * attempting to establish a connection to check if the input connection
     * settings are valid. This method DOES NOT establish a connection. Connect()
     * should be called to attempt to establish a connection. This method should
     * inspect the input ConnSettingRequestMap and return any modified and
     * additional requested connection settings in the returned
     * ConnSettingResponseMap.
     * <p>
     * If any connection settings have been modified, then an OPT_VAL_CHANGED
     * warning must be posted. If a key in the map is unrecognized or does not
     * belong in this stage of connection, then an INVALID_CONN_STR_ATTR warning
     * must be posted.
     *
     * @param requestMap Connection settings specified for this attempt to establish
     *                   connection.
     * @return Connection settings that have been modified, and additional
     *         connection settings requested which are not provided in the input
     *         ConnSettingRequestMap.
     * @see ConnSettingRequestMap
     * @see ConnSettingResponseMap
     * @throws BadAuthException if the inputs fail authentication.
     * @throws ErrorException   if an error occurs.
     */
    public ConnSettingResponseMap updateConnectionSettings(ConnSettingRequestMap requestMap)
            throws BadAuthException, ErrorException {
        LogUtilities.logFunctionEntrance(getConnectionLog());
        // TODO #6: Check Connection Settings. Use verifyRequiredSetting() for
        ConnSettingResponseMap responseMap = new ConnSettingResponseMap();
        verifyRequiredSetting(URPropertyKey.SERVER, requestMap, responseMap);
        verifyOptionalSetting(URPropertyKey.DEFAULTMAXCOLUMNSIZE, requestMap, responseMap);
        return responseMap;
    }

    /**
     * Called by reset() to rest subclass-specific state.
     * 
     * Note: m_connectionProperties will already have been reset to DSIConnection's
     * defaults.
     * 
     * @throws ErrorException If an error occurs.
     */
    @Override
    protected void doReset() throws ErrorException {
        setDefaultProperties();
    }

    /**
     * Loads the default property values for the connection.
     *
     * @throws ErrorException if an error occurs.
     */
    private void setDefaultProperties() throws ErrorException {
        // TODO #3: Set the connection properties. This allows you to override
        // any of the default
        // values that are set by DSIConnection.
        try {
            setProperty(ConnPropertyKey.DSI_SERVER_NAME, new Variant(Variant.TYPE_WSTRING, URDriver.DRIVER_NAME));

            setProperty(ConnPropertyKey.DSI_USER_NAME, new Variant(Variant.TYPE_WSTRING, "User"));

            setProperty(ConnPropertyKey.DSI_DBMS_NAME, new Variant(Variant.TYPE_WSTRING, "JSON Files"));

            setProperty(ConnPropertyKey.DSI_CONN_ACCESS_MODE,
                    new Variant(Variant.TYPE_UINT64, Long.valueOf(ConnPropertyValues.DSI_PROP_MODE_READ_ONLY)));

            // Stored procedures are not supported in this driver.
            PropertyUtilities.setStoredProcedureSupport(this, false);

            // Override defaults
            final long stringFunctionsToExclude = ConnPropertyValues.DSI_FN_STR_CHAR_LENGTH
                    | ConnPropertyValues.DSI_FN_STR_CHARACTER_LENGTH | ConnPropertyValues.DSI_FN_STR_OCTET_LENGTH
                    | ConnPropertyValues.DSI_FN_STR_POSITION;
            setProperty(ConnPropertyKey.DSI_STRING_FUNCTIONS, new Variant(Variant.TYPE_UINT32, Long
                    .valueOf(getProperty(ConnPropertyKey.DSI_STRING_FUNCTIONS).getLong() & ~stringFunctionsToExclude)));
            setProperty(ConnPropertyKey.DSI_CONN_STOP_ON_ERROR,
                    new Variant(Variant.TYPE_UINT32, Long.valueOf(ConnPropertyValues.DSI_SOE_MULTI_PARAM_SET)));
        } catch (Exception e) {
            // This should never happen.
            throw URDriver.s_QJMessages.createGeneralException(URMessageKey.CONN_DEFAULT_PROP_ERR.name(), e);
        }
    }
    
    private static boolean isFilePath(String input) {
        // Check if the string contains directory separators (e.g., "/", "\")
        if (input.contains("/") || input.contains("\\")) {
            // Check if the string ends with a file name extension (e.g., ".txt", ".jpg")
            return input.matches(".*\\.[^\\/\\\\]+$");
        }
        return false;
    }
}
