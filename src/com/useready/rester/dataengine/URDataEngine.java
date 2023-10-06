// =================================================================================================
///  @file QJDataEngine.java
///
///  Definition of the Class QJDataEngine
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.dataengine;

import com.simba.sqlengine.dsiext.dataengine.*;
import com.simba.sqlengine.dsiext.dataengine.metadata.DSIExtCatalogSchemasOnlyMetadataSource;
import com.simba.sqlengine.dsiext.dataengine.metadata.DSIExtCatalogsOnlyMetadataSource;
import com.simba.sqlengine.dsiext.dataengine.metadata.DSIExtColumnsMetadataSource;
import com.simba.sqlengine.dsiext.dataengine.metadata.DSIExtSchemasOnlyMetadataSource;
import com.simba.sqlengine.dsiext.dataengine.metadata.DSIExtTablesMetadataSource;
import com.simba.sqlengine.exceptions.SQLEngineException;
import com.simba.dsi.dataengine.impl.DSIEmptyMetadataSource;
import com.simba.dsi.dataengine.impl.DSITableTypeOnlyMetadataSource;
import com.simba.dsi.dataengine.interfaces.IMetadataSource;
import com.simba.dsi.dataengine.interfaces.IQueryExecutor;
import com.simba.dsi.dataengine.utilities.ColumnMetadata;
import com.simba.dsi.dataengine.utilities.DataWrapper;
import com.simba.dsi.dataengine.utilities.MetadataSourceColumnTag;
import com.simba.dsi.dataengine.utilities.MetadataSourceID;
import com.simba.dsi.exceptions.ParsingException;
import com.simba.dsi.utilities.DSIMessageKey;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ErrorException;
import com.useready.rester.Settings;
import com.useready.rester.core.URConnection;
import com.useready.rester.core.URCoreUtils;
import com.useready.rester.core.URDriver;
import com.useready.rester.core.URStatement;
import com.useready.rester.dataengine.metadata.*;
import com.useready.rester.dataengine.metadata.URTypeInfoMetadataSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of the data engine for QuickJson Driver.
 *
 * The QuickJson leverages Simba's Java SQL engine to perform the SQL
 * processing.
 */
public class URDataEngine extends SqlDataEngine {
	/*
	 * Instance Variable(s)
	 * ========================================================================
	 */

	/**
	 * The parent statement of the data engine.
	 */
	private URStatement m_statement = null;

	// private Settings m_setting=new Settings();
	private Settings m_settings;

	/*
	 * Constructor(s)
	 * =============================================================================
	 * =
	 */

	/**
	 * Constructor.
	 *
	 * @param statement The parent statement of the data engine.
	 *
	 * @throws ErrorException If an error occurs.
	 */
	public URDataEngine(URStatement statement) throws ErrorException {
		super(statement);
		LogUtilities.logFunctionEntrance(getLog(), statement);
		m_statement = statement;
		m_settings = ((URConnection) m_statement.getParentConnection()).m_settings;
	}

	/*
	 * Method(s)
	 * =============================================================================
	 * ======
	 */

	/**
	 * Create a helper object to retrieve basic metadata that can be used to provide
	 * default implementations of catalog metadata sources.
	 *
	 * The default implementation in this class returns null. That is, no default
	 * metadata is supported.
	 *
	 * @return A helper object to retrieve basic metadata if supported; null
	 *         otherwise.
	 */
	@Override
	public IMetadataHelper createMetadataHelper() {
		LogUtilities.logFunctionEntrance(m_statement.getLog());
		return new URMetadataHelper(m_statement.getParentConnection(), null, null);
	}

	/**
	 * Open a stored procedure.
	 *
	 * This method will be called during the preparation of a SQL statement.
	 *
	 * Once the stored procedure is opened, it should allow retrieval of metadata.
	 * That is, calling GetResults() on the returned procedure should return results
	 * that provide column metadata, if any, and calling GetParameters() on the
	 * returned procedure should return parameter metadata, if any.
	 *
	 * If a result set is returned, before data can be retrieved from the table
	 * SetCursorType() will have to called. Since this is done at the execution
	 * time, the DSII should _NOT_ try to make the data ready for retrieval until
	 * Execute() is called.
	 *
	 * The DSII decides how catalog and schema are interpreted or supported. In this
	 * example procedures are not supported so openProcedure() is not implemented.
	 *
	 * @param catalogName The name of the catalog in which the stored procedure
	 *                    resides.
	 * @param schemaName  The name of the schema in which the stored procedure
	 *                    resides.
	 * @param procName    The name of the stored procedure to open.
	 *
	 * @return the opened procedure, NULL if the procedure does not exist.
	 *
	 * @throws ErrorException If an error occurs.
	 */
	@Override
	public StoredProcedure openProcedure(String catalogName, String schemaName, String procName) throws ErrorException {
		return URProcedureFactory.open(catalogName, schemaName, procName, m_statement.getLog(),
				m_statement.getWarningListener());
	}

	/**
	 * Open a physical table/view.
	 *
	 * This method will be called during the preparation of a SQL statement.
	 *
	 * Once the table is opened, it should allow retrieving of metadata. That is,
	 * calling GetSelectColumns() on the returned table should return column
	 * metadata. SimbaEngine needs the table metadata to infer the column metadata
	 * of the result set if the SQL statement is a query.
	 *
	 * Before data can be retrieved from the table, SetCursorType() needs to be
	 * called. Since this is done at the execution time, the DSII should _NOT_ try
	 * to make the data ready for retrieval until SetCursorType() is called.
	 *
	 * The DSII decides how catalog and schema are interpreted or supported. If the
	 * same table is asked to open twice (that is, OpenTable() is called twice), the
	 * DSII _MUST_ return two separate IResultSet instances since two cursors will
	 * be needed.
	 *
	 * SimbaEngine will ensure this method is called only once if the table is
	 * referenced only once in a SQL statement.
	 *
	 * @param catalogName The name of the catalog in which the table resides. If no
	 *                    catalog is provided then it will be given as a empty
	 *                    string
	 * @param schemaName  The name of the schema in which the table resides.
	 * @param tableName   The name of the table to open.
	 * @param openType    An enum indicating how the table should be opened. Default
	 *                    is read-only.
	 *
	 * @return the opened table, NULL if the table does not exist.
	 *
	 * @throws ErrorException If any other error occurs.
	 */
	@Override
	public DSIExtJResultSet openTable(String catalogName, String schemaName, String tableName, OpenTableType openType)
			throws ErrorException {
		LogUtilities.logFunctionEntrance(getLog(), catalogName, schemaName, tableName, openType);

		// TODO #9: Open A Table.

		Identifier tableIdentifier = new Identifier();
		if (URCoreUtils.findTableInMapping(catalogName, schemaName, tableName,
				((URConnection) m_statement.getParentConnection()).m_settings.m_tableMap, tableIdentifier, getLog())) {
			// LogUtilities.logDebug("---------------------find Table in mapping",
			// getLog());
			return new URTable(getLog(), ((URConnection) m_statement.getParentConnection()).m_settings,
					tableIdentifier);
		} else {
			// LogUtilities.logDebug("---------------------find Table in mapping else",
			// getLog());
			return null;
		}
	}

	/**
	 * Creates a metadata source which may filter out unneeded rows according to the
	 * given restrictions.
	 *
	 * @param metadataSourceId    Identifier to create the appropriate metadata
	 *                            source.
	 * @param restrictions        The restrictions to be applied to the metadata
	 *                            table. Only columns that have restrictions appear
	 *                            in the collection of restrictions.
	 * @param escapeChar          Escape character used in filtering.
	 * @param identifierQuoteChar Character used as a quote around identifiers.
	 * @param filterAsIdentifier  Indicates if string filters are treated as
	 *                            identifiers.
	 *
	 * @return An IMetadataSource object representing the requested metadata.
	 *
	 * @throws ErrorException If an error occurs.
	 */
	@Override
	protected IMetadataSource makeNewMetadataSource(MetadataSourceID metadataSourceId,
			Map<MetadataSourceColumnTag, String> restrictions, String escapeChar, String identifierQuoteChar,
			boolean filterAsIdentifier) throws ErrorException {
		LogUtilities.logFunctionEntrance(getLog(), metadataSourceId, restrictions, escapeChar, identifierQuoteChar,
				filterAsIdentifier);
		// TODO #8: Create and return your Metadata Sources. Metadata Sources correspond
		// to the
		// result sets that are returned when calling a Catalog function.
		// E.g. SQLTables, SQLColumns.

		/*
		 * At the very least, JDBC conforming applications will require the following
		 * metadata sources:
		 * 
		 * DSI_TABLES_METADATA List of all tables defined in the data source.
		 * 
		 * DSI_CATALOGONLY_METADATA List of all catalogs defined in the data source.
		 * 
		 * DSI_SCHEMA_METADATA List of all schemas defined in the data source.
		 * 
		 * DSI_TABLETYPEONLY_METADATA List of all table types (SYSTEM TABLE,TABLE,VIEW)
		 * defined within the data source.
		 * 
		 * DSI_COLUMNS_METADATA List of all columns defined across all tables in the
		 * data source.
		 * 
		 * DSI_TYPE_INFO_METADATA List of the supported types by the data source.
		 * 
		 * In some cases applications may provide values to restrict the metadata
		 * sources. These restrictions are stored within in_restrictions and can be used
		 * by you to restrict the number of rows that are returned from the metadata
		 * source.
		 */
		Settings settings = new Settings();
		settings.setM_logger(getLog());
		URMetadataHelper metadataHelper = new URMetadataHelper(getParentStatement().getParentConnection(), restrictions,
				metadataSourceId);
		switch (metadataSourceId) {
		case TYPE_INFO: {
			return new URTypeInfoMetadataSource(getLog(), getIsODBC2());
		}

		case CATALOG_SCHEMA_ONLY: {
			return new DSIExtCatalogSchemasOnlyMetadataSource(getLog(), restrictions, metadataHelper);
		}

		case SCHEMA_ONLY: {
			return new DSIExtSchemasOnlyMetadataSource(getLog(), restrictions, metadataHelper);
		}

		case CATALOG_ONLY: {
			return new DSIExtCatalogsOnlyMetadataSource(getLog(), metadataHelper);
		}

		case COLUMNS: {
			return new DSIExtColumnsMetadataSource(getLog(), restrictions, metadataHelper, this);
		}

		case TABLES: {
			return new DSIExtTablesMetadataSource(getLog(), restrictions, metadataHelper, getIsODBC2());
		}

		case TABLETYPE_ONLY: {
			return new DSITableTypeOnlyMetadataSource(getLog());
		}

		default: {
			return new DSIEmptyMetadataSource(getLog());
		}
		}
	}

	public IQueryExecutor prepare(String query) throws ParsingException, ErrorException {
		LogUtilities.logFunctionEntrance(getLog(), query);
		m_settings.m_queryRun = true;
		m_settings.m_columnData=new HashMap<>();
		try {
			String fieldName = null;
			if (m_settings.m_format.equalsIgnoreCase("loadOnDemand")) {
				Map<String, String> columnNamesMap = new HashMap<String, String>();
				columnNamesMap = SQLParser.extractColumnNameSQLParser(query, m_settings,getLog());
				LogUtilities.logDebug(" Column Name Map:::::::::::::::: " + columnNamesMap, getLog());
				
				LogUtilities.logDebug("m_settings.m_columnData:::::::::::: " + m_settings.m_columnData, getLog());
				for (Map.Entry<String, String> entry : columnNamesMap.entrySet()) {
					LogUtilities.logDebug("Alias: " + entry.getKey() + ", Column Name: " + entry.getValue(), getLog());
					try {
						if (fieldName == null) {
							fieldName = "\"" + entry.getValue() + "\"";
						} else {
							fieldName = fieldName + "," + "\"" + entry.getValue().replace("\n", "").replace(",", "")
									+ "\"";
						}
					} catch (Exception e) {
						LogUtilities.logError(e, getLog());
					}

				}
				LogUtilities.logDebug("--- str1---" + fieldName, getLog());
			}
			if (m_settings.m_format.equals("oneCall")) {
				m_settings.originalColumnNames = new ArrayList<String>();
				String pattern = "\"([^\"]+)\"\\s+AS\\s+\"([^\"]+)\"";
				Pattern columnPattern = Pattern.compile(pattern);

				Matcher matcher = columnPattern.matcher(query);

				while (matcher.find()) {
					String originalColumnName = matcher.group(1);
					m_settings.originalColumnNames.add(originalColumnName);
				}
				fetchFilterData(query);
			}
			fieldName = "[" + fieldName + "]";
			m_settings.originalColumnNames = null;
			m_settings.str = fieldName;
			return super.prepare(query);
		} catch (SQLEngineException e) {
			// TODO Auto-generated catch block
			throw e;
		} catch (Exception ex) {
			throw ex;
		}

	}

	private void fetchFilterData(String query) {
		m_settings.m_filter = null;
		// TODO Auto-generated method stub
		Pattern pattern = Pattern.compile("WHERE\\s+(.*?)\\s+GROUP\\s+BY", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		Matcher matcher = pattern.matcher(query);

		if (matcher.find()) {
			String whereClause = matcher.group(1);

			// Extract individual conditions using another pattern
			Pattern conditionPattern = Pattern.compile("\"([^\"]+)\"\\s*=\\s*'([^']+)'");
			Matcher conditionMatcher = conditionPattern.matcher(whereClause);
			String str = null;
			while (conditionMatcher.find()) {
				String columnName = conditionMatcher.group(1);
				String value = conditionMatcher.group(2);
				int lastDotIndex = columnName.lastIndexOf('.');
				String name = columnName.substring(lastDotIndex + 1);
				if (str == null) {
					str = "\"" + name + "\"" + ": \"" + value + "\"";
				} else {
					str = str + "," + "\"" + name + "\"" + ": \"" + value + "\"";
				}

			}
			m_settings.m_filter = str;

		}
	}

	@Override
	public void onBeginExecution(IExecutionContext contaxt) {
		LogUtilities.logFunctionEntrance(getLog(), contaxt);
	}

	public void SortedTemporaryTable(ColumnMetadata metadata) {
		LogUtilities.logFunctionEntrance(getLog(), metadata);
	}
}
