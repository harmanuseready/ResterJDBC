package com.useready.rester.vaild;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;

import java.util.ArrayList;
import java.util.List;

/**
 * This java class was created for testing basic JDBC calls.
 * 
 * <Note that not all the functions are required to pass,
 * as features might be different across all data sources.>
 * 
 */
public class JDBCTester
{    
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		Connection conn = makeConnection("jdbc:useready://localhost;config=D:\\JDBCDriver29Aug\\ResterZinc.json;LogLevel=6;LogPath=D:\\\\Logs;server=PROD","com.useready.rester.core.jdbc42.URJDBC42Driver");
        //getProcedures(conn);
	}
    /**
     * Run all the catalog functions.
     * 
     * @param conn                      The connection.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void allCatalogFunctions4(Connection conn) throws SQLException
    {
        getCatalogs(conn);
        getSchemas(conn, null, null);
        getTables(conn, null, null, null, null);
        getColumns4(conn, null, null, null, null);
        getPrimaryKeys(conn);
        getForeginKeys(conn);
        getProcedures(conn);
    }

    /**
     * Run all the catalog functions.
     *
     * @param conn                      The connection.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void allCatalogFunctions41(Connection conn) throws SQLException
    {
        getCatalogs(conn);
        getSchemas(conn, null, null);
        getTables(conn, null, null, null, null);
        getColumns41(conn, null, null, null, null);
        getPrimaryKeys(conn);
        getForeginKeys(conn);
        getProcedures(conn);
    }
    
    /**
     * Establish connection to Database with provided connection string.
     * 
     * @param connString                The connection string.
     *
     * @return                          Connection object after connecting with the datasource.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static Connection makeConnection(String connString) throws SQLException
    {
        System.out.println("Current Connection info: " + connString);

        return DriverManager.getConnection(connString);
    }

    /**
     * Establish connection to Database with provided connection string and driver class path.
     * 
     * @param connString                The connection string.
     * @param className                 The complete driver class path.
     *
     * @return                          Connection object after connecting with the datasource.
     *
     * @throws ClassNotFoundException   Classpath exception if no driver class found.
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static Connection makeConnection(
        String connString, 
        String className) throws ClassNotFoundException, SQLException
    {
        System.out.println("Current Connection info: " + connString);

        Class.forName(className);
        System.out.println("Current driver info: " + className);
        return DriverManager.getConnection(connString);
    }

    /**
     * Run all the API functions.
     *
     * @param conn                      The connection.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void testAPI(Connection conn) throws SQLException
    {
        System.out
        .println("-----------------------------------------------------------------------");
        
        System.out.println("Connection API Calls:");
        
        System.out.println("ConnectionAutoCommit: " + conn.getAutoCommit());
        System.out.println("ConnectionIsReadOnly: " + conn.isReadOnly());
        System.out.println("ConnectionGetCatalog: " + conn.getCatalog());
        System.out.println("ConnectionTransactionIsolation: " + conn.getTransactionIsolation());
        System.out.println("ConnectionGetHoldability: " + conn.getHoldability());
        
        System.out
        .println("-----------------------------------------------------------------------");
        
        System.out.println("Databases Metadata API Calls:");
        
        DatabaseMetaData md = conn.getMetaData();

        System.out.println("allProceduresAreCallable: " + md.allProceduresAreCallable());
        
        System.out.println("allTablesAreSelectable: " + md.allTablesAreSelectable());
        
        System.out.println("autoCommitFailureClosesAllResultSets: " 
            + md.autoCommitFailureClosesAllResultSets());
        
        System.out.println("dataDefinitionCausesTransactionCommit: " 
            + md.dataDefinitionCausesTransactionCommit());
        
        System.out.println("dataDefinitionIgnoredInTransactions: " 
            + md.dataDefinitionIgnoredInTransactions());
        
        System.out.println("deletesAreDetected: " 
            + md.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("doesMaxRowSizeIncludeBlobs: " + md.doesMaxRowSizeIncludeBlobs());
        
        System.out.println("CatalogSeparator: " + md.getCatalogSeparator());
        
        System.out.println("CatalogTerm: " + md.getCatalogTerm());
        
        System.out.println("DatabaseMajorVersion: " + md.getDatabaseMajorVersion());
        
        System.out.println("DatabaseMinorVersion : " + md.getDatabaseMinorVersion());
        
        System.out.println("DatabaseProductName: " + md.getDatabaseProductName());
        
        System.out.println("DatabaseProductVersion: " + md.getDatabaseProductVersion());
        
        System.out.println("DefaultTransactionIsolation: " + md.getDefaultTransactionIsolation());
        
        System.out.println("DriverMajorVersion: " + md.getDriverMajorVersion());
        
        System.out.println("DriverMinorVersion: " + md.getDriverMinorVersion());
        
        System.out.println("DriverName: " + md.getDriverName());
        
        System.out.println("DriverVersion: " + md.getDriverVersion());

        System.out.println("ExtraNameCharacters: " + md.getExtraNameCharacters());
        
        System.out.println("IdentifierQuoteString: " + md.getIdentifierQuoteString());
        
        System.out.println("JDBCMajorVersion: " + md.getJDBCMajorVersion());
        
        System.out.println("JDBCMinorVersion: " + md.getJDBCMinorVersion());
        
        System.out.println("MaxBinaryLiteralLength: " + md.getMaxBinaryLiteralLength());
        
        System.out.println("MaxCatalogNameLength: " + md.getMaxCatalogNameLength());
        
        System.out.println("MaxCharLiteralLength: " + md.getMaxCharLiteralLength());
        
        System.out.println("MaxColumnNameLength: " + md.getMaxColumnNameLength());
        
        System.out.println("MaxColumnsInGroupBy: " + md.getMaxColumnsInGroupBy());
        
        System.out.println("MaxColumnsInIndex: " + md.getMaxColumnsInIndex());
        
        System.out.println("MaxColumnsInOrderBy: " + md.getMaxColumnsInOrderBy());
        
        System.out.println("MaxColumnsInSelect: " + md.getMaxColumnsInSelect());
        
        System.out.println("MaxColumnsInTable: " + md.getMaxColumnsInTable());
        
        System.out.println("MaxConnections: " + md.getMaxConnections());
        
        System.out.println("MaxCursorNameLength: " + md.getMaxCursorNameLength());
        
        System.out.println("MaxIndexLength: " + md.getMaxIndexLength());
        
        System.out.println("MaxProcedureNameLength: " + md.getMaxProcedureNameLength());
        
        System.out.println("MaxRowSize: " + md.getMaxRowSize());
        
        System.out.println("MaxSchemaNameLength: " + md.getMaxSchemaNameLength());
        
        System.out.println("MaxStatementLength: " + md.getMaxStatementLength());
        
        System.out.println("MaxStatements: " + md.getMaxStatements());
        
        System.out.println("MaxTableNameLength: " + md.getMaxTableNameLength());
        
        System.out.println("MaxTablesInSelect: " + md.getMaxTablesInSelect());
        
        System.out.println("MaxUserNameLength: " + md.getMaxUserNameLength());
        
        System.out.println("NumericFunctions: " + md.getNumericFunctions());
        
        System.out.println("ProcedureTerm: " + md.getProcedureTerm());
        
        System.out.println("ResultSetHoldability: " + md.getResultSetHoldability());
        
        System.out.println("SchemaTerm: " + md.getSchemaTerm());
        
        System.out.println("SearchStringEscape: " + md.getSearchStringEscape());
        
        System.out.println("SQLKeywords: " + md.getSQLKeywords());
        
        System.out.println("SQLStateType: " + md.getSQLStateType());
        
        System.out.println("StringFunctions: " + md.getStringFunctions());
        
        System.out.println("SystemFunctions: " + md.getSystemFunctions());
        
        System.out.println("TimeDateFunctions: " + md.getTimeDateFunctions());
        
        System.out.println("URL: " + md.getURL());
        
        System.out.println("UserName: " + md.getUserName());
        
        System.out.println("insertsAreDetected: " 
            + md.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("isCatalogAtStart: " + md.isCatalogAtStart());
        
        System.out.println("isReadOnly: " + md.isReadOnly());
        
        System.out.println("locatorsUpdateCopy: " + md.locatorsUpdateCopy());
        
        System.out.println("nullPlusNonNullIsNull: " + md.nullPlusNonNullIsNull());
        
        System.out.println("nullsAreSortedAtEnd: " + md.nullsAreSortedAtEnd());
        
        System.out.println("nullsAreSortedAtStart: " + md.nullsAreSortedAtStart());
        
        System.out.println("nullsAreSortedHigh: " + md.nullsAreSortedHigh());
        
        System.out.println("nullsAreSortedLow: " + md.nullsAreSortedLow());
        
        System.out.println("othersDeletesAreVisible: "
            + md.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("othersInsertsAreVisible: " 
            + md.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("othersUpdatesAreVisible: " 
            + md.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("ownDeletesAreVisible: " 
            + md.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("ownInsertsAreVisible: " 
            + md.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("ownUpdatesAreVisible: " 
            + md.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("storesLowerCaseIdentifiers: " + md.storesLowerCaseIdentifiers());
        
        System.out.println("storesLowerCaseQuotedIdentifiers: " 
            + md.storesLowerCaseQuotedIdentifiers());
        
        System.out.println("storesMixedCaseIdentifiers: " + md.storesMixedCaseIdentifiers());
        
        System.out.println("storesMixedCaseQuotedIdentifiers: " 
            + md.storesMixedCaseQuotedIdentifiers());
        
        System.out.println("storesUpperCaseIdentifiers: " + md.storesUpperCaseIdentifiers());
        
        System.out.println("storesUpperCaseQuotedIdentifiers: " 
            + md.storesUpperCaseQuotedIdentifiers());
        
        System.out.println("supportsAlterTableWithAddColumn: " 
            + md.supportsAlterTableWithAddColumn());
        
        System.out.println("supportsAlterTableWithDropColumn: " 
            + md.supportsAlterTableWithDropColumn());
        
        System.out.println("supportsANSI92EntryLevelSQL: " + md.supportsANSI92EntryLevelSQL());
        
        System.out.println("supportsANSI92FullSQL: " + md.supportsANSI92FullSQL());
        
        System.out.println("supportsANSI92IntermediateSQL: " + md.supportsANSI92IntermediateSQL());
        
        System.out.println("supportsBatchUpdates: " + md.supportsBatchUpdates());
        
        System.out.println("supportsCatalogsInDataManipulation: " 
            + md.supportsCatalogsInDataManipulation());
        
        System.out.println("supportsCatalogsInIndexDefinitions: "
            + md.supportsCatalogsInIndexDefinitions());
        
        System.out.println("supportsCatalogsInPrivilegeDefinitions: " 
            + md.supportsCatalogsInPrivilegeDefinitions());
        
        System.out.println("supportsCatalogsInProcedureCalls: " 
            + md.supportsCatalogsInProcedureCalls());
        
        System.out.println("supportsCatalogsInTableDefinitions: " 
            + md.supportsCatalogsInTableDefinitions());
        
        System.out.println("supportsColumnAliasing: " + md.supportsColumnAliasing());
        
        System.out.println("supportsConvert: " + md.supportsConvert());
        
        System.out.println("supports Convert from BIGINT to VARCHAR: "
            + md.supportsConvert(Types.BIGINT, Types.VARCHAR));
        
        System.out.println("supports Convert from BIGINT to VARBINARY: "
            + md.supportsConvert(Types.BIGINT, Types.VARBINARY));
        
        System.out.println("supportsCoreSQLGrammar: " + md.supportsCoreSQLGrammar());
        
        System.out.println("supportsCorrelatedSubqueries: " + md.supportsCorrelatedSubqueries());
        
        System.out.println("supportsDataDefinitionAndDataManipulationTransactions: " 
            + md.supportsDataDefinitionAndDataManipulationTransactions());
        
        System.out.println("supportsDataManipulationTransactionsOnly: "
            + md.supportsDataManipulationTransactionsOnly());
        
        System.out.println("supportsDifferentTableCorrelationNames: " 
            + md.supportsDifferentTableCorrelationNames());
        
        System.out.println("supportsExpressionsInOrderBy: " + md.supportsExpressionsInOrderBy());
        
        System.out.println("supportsExtendedSQLGrammar: " + md.supportsExtendedSQLGrammar());
        
        System.out.println("supportsFullOuterJoins: " + md.supportsFullOuterJoins());
        
        System.out.println("supportsGetGeneratedKeys: " + md.supportsGetGeneratedKeys());
        
        System.out.println("supportsGroupBy: " + md.supportsGroupBy());
        
        System.out.println("supportsGroupByBeyondSelect: " + md.supportsGroupByBeyondSelect());
        
        System.out.println("supportsGroupByUnrelated: " + md.supportsGroupByUnrelated());
        
        System.out.println("supportsIntegrityEnhancementFacility: "
            + md.supportsIntegrityEnhancementFacility());
        
        System.out.println("supportsLikeEscapeClause: " + md.supportsLikeEscapeClause());
        
        System.out.println("supportsLimitedOuterJoins: " + md.supportsLimitedOuterJoins());
        
        System.out.println("supportsMinimumSQLGrammar: " + md.supportsMinimumSQLGrammar());
        
        System.out.println("supportsMixedCaseIdentifiers: " + md.supportsMixedCaseIdentifiers());
        
        System.out.println("supportsMixedCaseQuotedIdentifiers: " 
            + md.supportsMixedCaseQuotedIdentifiers());
        
        System.out.println("supportsMultipleOpenResults: " + md.supportsMultipleOpenResults());
        
        System.out.println("supportsMultipleResultSets: " + md.supportsMultipleResultSets());
        
        System.out.println("supportsMultipleTransactions: " + md.supportsMultipleTransactions());
        
        System.out.println("supportsNamedParameters: " + md.supportsNamedParameters());
        
        System.out.println("supportsNonNullableColumns: " + md.supportsNonNullableColumns());
        
        System.out.println("supportsOpenCursorsAcrossCommit: " 
            + md.supportsOpenCursorsAcrossCommit());
        
        System.out.println("supportsOpenCursorsAcrossRollback: " 
            + md.supportsOpenCursorsAcrossRollback());
        
        System.out.println("supportsOpenStatementsAcrossCommit: " 
            + md.supportsOpenStatementsAcrossCommit());
        
        System.out.println("supportsOpenStatementsAcrossRollback: " 
            + md.supportsOpenStatementsAcrossRollback());
        
        System.out.println("supportsOrderByUnrelated: " + md.supportsOrderByUnrelated());
        
        System.out.println("supportsOuterJoins: " + md.supportsOuterJoins());
        
        System.out.println("supportsPositionedDelete: " + md.supportsPositionedDelete());
        
        System.out.println("supportsPositionedUpdate: " + md.supportsPositionedUpdate());
        
        System.out.println("supportsResultSetConcurrency: " 
            + md.supportsResultSetConcurrency(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.FETCH_UNKNOWN));
        
        System.out.println("supportsResultSetHoldability: " 
            + md.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
        
        System.out.println("supportsResultSetHoldability: " 
            + md.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
        
        System.out.println("supportsResultSetType: " 
            + md.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("supportsResultSetType: " 
            + md.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
        
        System.out.println("supportsResultSetType: " 
            + md.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
        
        System.out.println("supportsSavepoints: " + md.supportsSavepoints());
        
        System.out.println("supportsSchemasInDataManipulation: " 
            + md.supportsSchemasInDataManipulation());
        
        System.out.println("supportsSchemasInIndexDefinitions: " 
            + md.supportsSchemasInIndexDefinitions());
        
        System.out.println("supportsSchemasInPrivilegeDefinitions: " 
            + md.supportsSchemasInPrivilegeDefinitions());
        
        System.out.println("supportsSchemasInProcedureCalls: " 
            + md.supportsSchemasInProcedureCalls());
        
        System.out.println("supportsSchemasInTableDefinitions: " 
            + md.supportsSchemasInTableDefinitions());
        
        System.out.println("supportsSelectForUpdate: " + md.supportsSelectForUpdate());
        
        System.out.println("supportsStatementPooling: " + md.supportsStatementPooling());
        
        System.out.println("supportsStoredFunctionsUsingCallSyntax: " 
            + md.supportsStoredFunctionsUsingCallSyntax());
        
        System.out.println("supportsStoredProcedures: " + md.supportsStoredProcedures());
        
        System.out.println("supportsSubqueriesInComparisons: " 
            + md.supportsSubqueriesInComparisons());
        
        System.out.println("supportsSubqueriesInExists: " + md.supportsSubqueriesInExists());
        
        System.out.println("supportsSubqueriesInIns: " + md.supportsSubqueriesInIns());
        
        System.out.println("supportsSubqueriesInQuantifieds: " 
            + md.supportsSubqueriesInQuantifieds());
        
        System.out.println("supportsTableCorrelationNames: " + md.supportsTableCorrelationNames());
        
        System.out.println("supportsTransactionIsolationLevel: " 
            + md.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
        
        System.out.println("supportsTransactionIsolationLevel: " 
            + md.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
        
        System.out.println("supportsTransactionIsolationLevel: " 
            + md.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
        
        System.out.println("supportsTransactionIsolationLevel: " 
            + md.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
        
        System.out.println("supportsTransactions: " + md.supportsTransactions());
        
        System.out.println("supportsUnion: " + md.supportsUnion());
        
        System.out.println("supportsUnionAll: " + md.supportsUnionAll());
        
        System.out.println("updatesAreDetected: " 
            + md.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
        
        System.out.println("usesLocalFilePerTable: " + md.usesLocalFilePerTable());
        
        System.out.println("usesLocalFiles: " + md.usesLocalFiles());
    }
    
    /**
     * Test cancel during fetch.
     *
     * @param conn                      The connection.
     * @param query                     The input query.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void testCancelDuringFetch(Connection conn, String query) throws SQLException
    {
        System.out
        .println("-----------------------------------------------------------------------");
        
        final Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery(query);
        
        // Print the column names.
        ResultSetMetaData rsmd = result.getMetaData();
        printResultSetMetadata(rsmd);
        
        Thread t1 = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                System.out.println("Thread 1");
                try
                {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                
                try
                {
                    statement.cancel();
                }
                catch (SQLException e)
                {
                    e.printStackTrace();
                }
            }
        });
        
        t1.start();
        
        System.out.println("");
        System.out.println("Table result: ");
        printResultSet(result);

        result.close();
        statement.close();
        System.out.println(
                "-----------------------------------------------------------------------");
    }

    /**
     * Test prepare.
     *
     * @param conn                      The connection.
     * @param query                     The input query.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void testPrepare(Connection conn, String query) throws SQLException
    {
        PreparedStatement Test = null;
        try
        {
            Test = conn.prepareStatement(query);

            System.out.println("Query Prepared");
            ResultSetMetaData rsmd = Test.getMetaData();
            printResultSetMetadata(rsmd);
            
            System.out.println("Query executed");
            ResultSet result = Test.executeQuery();
            
            System.out.println("Executed Column metadata: ");
            ResultSetMetaData rsmdrs = result.getMetaData();
            printResultSetMetadata(rsmdrs);
            
            System.out.println("");
            System.out.println("Table result: ");
            printResultSet(result);
            
            result.close();
            Test.close();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            if (conn != null)
            {
                try
                {
                    System.err.print("Transaction is being rolled back");
                    conn.rollback();
                }
                catch (SQLException excep)
                {
                    excep.printStackTrace();
                }
            }
        }
        finally
        {
            if (Test != null)
            {
                Test.close();
            }
            conn.setAutoCommit(true);
        }
    }

    /**
     * Test prepare with parameter.
     *
     * @param conn                      The connection.
     * @param query                     The input query.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void testPrepareWithParameter(Connection conn, String query)
            throws SQLException
    {
        PreparedStatement Test = null;
        try
        {
            Test = conn.prepareStatement(query);

            System.out.println("Query Prepared");
            ResultSetMetaData rsmd = Test.getMetaData();
            printResultSetMetadata(rsmd);
            
            ParameterMetaData pmmd = Test.getParameterMetaData();
            System.out.println("Query plan Parameter COUNT is: " + pmmd.getParameterCount());
            System.out.println("Query plan Parameter metadata: ");
            for (int i = 1; i <= pmmd.getParameterCount(); ++i)
            {
                System.out.print(
                    pmmd.getParameterTypeName(i) + "|" 
                        + pmmd.getParameterType(i) + "|"
                        + pmmd.getParameterClassName(i) + "|");
                System.out.println("");
            }

            // TODO: Set Parameter Here
            // Base on the parameter type, set value
            // e.g. Test.setString(1, "aa");
            /**
             * Test.setString(1, "aa");
             * Test.setInt(2, 2);
             */

            System.out.println("");
            System.out.println("Query executed");
            ResultSet result = Test.executeQuery();
            
            System.out.println("Executed Column metadata: ");
            ResultSetMetaData rsmdrs = result.getMetaData();
            printResultSetMetadata(rsmdrs);
            
            System.out.println("");
            System.out.println("Table result: ");
            printResultSet(result);
   
            result.close();
            Test.close();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            if (conn != null)
            {
                try
                {
                    System.err.print("Transaction is being rolled back");
                    conn.rollback();
                }
                catch (SQLException excep)
                {
                    excep.printStackTrace();
                }
            }
        }
        finally
        {
            if (Test != null)
            {
                Test.close();
            }
            conn.setAutoCommit(true);
        }
    }

    /**
     * Test query.
     *
     * @param conn                      The connection.
     * @param query                     The input query.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void testQuery(Connection conn, String query) throws SQLException
    {
        System.out
        .println("-----------------------------------------------------------------------");
        Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery(query);
        
        // Print the column names.
        ResultSetMetaData rsmd = result.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        System.out.println("Table result: ");
        printResultSet(result);

        SQLWarning warning = statement.getWarnings();
        if (warning != null)
        {
            System.out.println("---Warning---");
            while (warning != null)
            {
                System.out.println("Message: " + warning.getMessage());
                System.out.println("SQLState: " + warning.getSQLState());
                System.out.print("Vendor error code: ");
                System.out.println(warning.getErrorCode());
                System.out.println("");
                warning = warning.getNextWarning();
            }
        }
        result.close();
        statement.close();
        System.out.println(
                "-----------------------------------------------------------------------");
    }

    /**
     * Test DML update query.
     *
     * @param conn                      The connection.
     * @param query                     The input query.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void testUpdate(Connection conn, String query) throws SQLException
    {
        System.out.println(
                "-----------------------------------------------------------------------");
        System.out.println("Execute update query: " + query);
        Statement statement = conn.createStatement();
        int result = statement.executeUpdate(query);
        System.out.println(result + " rows get updated");
        System.out.println(
                "-----------------------------------------------------------------------");
        statement.close();
    }

    /**
     * Test getCatalogs.
     *
     * @param conn                      The connection.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getCatalogs(Connection conn) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");
        DatabaseMetaData md = conn.getMetaData();
        System.out.println("Test get catalog Name:");
        ResultSet resultSet_Catalog = md.getCatalogs();
        
        ResultSetMetaData rsmd = resultSet_Catalog.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        printResultSet(resultSet_Catalog);
        
        resultSet_Catalog.close();
        System.out.println("-----------------------------------------------------------------------");
    }

    /**
     * Test getColumns for JDBC4, configuration is available to select wanted data.
     *
     * @param conn                      The connection.
     * @param catalogName               The catalog name.
     * @param schemaPattern             The schema pattern.
     * @param tablePattern              The table pattern.
     * @param columnPattern             The column pattern.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getColumns4(
        Connection conn,
        String catalogName,
        String schemaPattern,
        String tablePattern,
        String columnPattern) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");

        DatabaseMetaData md = conn.getMetaData();
        System.out.println("Test get columns");
        ResultSet resultSet_column = md.getColumns(
            catalogName,
            schemaPattern,
            tablePattern,
            columnPattern);
        
        ResultSetMetaData rsmd = resultSet_column.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        printResultSet(resultSet_column);
        
        resultSet_column.close();
        System.out.println("-----------------------------------------------------------------------");
    }

    /**
     * Test getColumns for JDBC41, configuration is available to select wanted data.
     *
     * @param conn                      The connection.
     * @param catalogName               The catalog name.
     * @param schemaPattern             The schema pattern.
     * @param tablePattern              The table pattern.
     * @param columnPattern             The column pattern.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getColumns41(
        Connection conn,
        String catalogName,
        String schemaPattern,
        String tablePattern,
        String columnPattern) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");

        DatabaseMetaData md = conn.getMetaData();
        System.out.println("Test get columns");
        ResultSet resultSet_column = md.getColumns(
            catalogName,
            schemaPattern,
            tablePattern,
            columnPattern);

        ResultSetMetaData rsmd = resultSet_column.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        printResultSet(resultSet_column);
        
        resultSet_column.close();
        System.out.println("-----------------------------------------------------------------------");
    }

    /**
     * Test getForeginKeys, configuration is available to select wanted data.
     *
     * @param conn                      The connection.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getForeginKeys(Connection conn) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");
        // optional configuration for call filter 
        String catalogRestriction = null;
        String schemaRestriction = null;
        String tableRestriction = null;

        DatabaseMetaData md = conn.getMetaData();
        System.out.println("Test get foreign keys");
        ResultSet resultSet_column = md.getImportedKeys(
                catalogRestriction,
                schemaRestriction,
                tableRestriction);
        
        ResultSetMetaData rsmd = resultSet_column.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        printResultSet(resultSet_column);

        resultSet_column.close();
        System.out.println("-----------------------------------------------------------------------");
    }

    /**
     * Test getPrimaryKeys, configuration is available to select wanted data.
     *
     * @param conn                      The connection.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getPrimaryKeys(Connection conn) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");
        // optional configuration for call filter 
        String catalogRestriction = null;
        String schemaRestriction = null;
        String tableRestriction = null;

        DatabaseMetaData md = conn.getMetaData();
        System.out.println("Test get primary keys");
        ResultSet resultSet_column = md.getPrimaryKeys(
                catalogRestriction,
                schemaRestriction,
                tableRestriction);

        ResultSetMetaData rsmd = resultSet_column.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        printResultSet(resultSet_column);
        
        resultSet_column.close();
        System.out.println("-----------------------------------------------------------------------");
    }

    /**
     * Test getProcedures, configuration is available to select wanted data.
     *
     * @param conn                      The connection.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getProcedures(Connection conn) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");
        // optional configuration for call filter 
        String catalogRestriction = null;
        String schemaRestriction = null;
        String procedureNameRestriction = null;

        DatabaseMetaData md = conn.getMetaData();
        System.out.println("Test get primary keys");
        ResultSet resultSet_column = md.getProcedures(
                catalogRestriction,
                schemaRestriction,
                procedureNameRestriction);

        ResultSetMetaData rsmd = resultSet_column.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        printResultSet(resultSet_column);
        
        resultSet_column.close();
        System.out.println("-----------------------------------------------------------------------");
    }

    /**
     * Test getSchemas, configuration is available to select wanted data.
     *
     * @param conn                      The connection.
     * @param catalogName               The catalog name.
     * @param schemaPattern             The schema pattern.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getSchemas(
        Connection conn, 
        String catalogName, 
        String schemaPattern) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");

        DatabaseMetaData md = conn.getMetaData();
        System.out.println("Test get schemas:");
        ResultSet resultSet_Schema = md.getSchemas(
            catalogName,
            schemaPattern);
        
        ResultSetMetaData rsmd = resultSet_Schema.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        printResultSet(resultSet_Schema);
        
        resultSet_Schema.close();
        System.out.println("-----------------------------------------------------------------------");
    }

    /**
     * Test getTables, configuration is available to select wanted data.
     *
     * @param conn                      The connection.
     * @param catalogName               The catalog name.
     * @param schemaPattern             The schema pattern.
     * @param tablePattern              The table pattern.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getTables(
        Connection conn,
        String catalogName,
        String schemaPattern,
        String tablePattern,
        String[] tableTypes) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");
        System.out.println("Test get tables");

        DatabaseMetaData md = conn.getMetaData();
        ResultSet resultSet_table = 
            md.getTables(catalogName, schemaPattern, tablePattern, tableTypes);
        
        ResultSetMetaData rsmd = resultSet_table.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        printResultSet(resultSet_table);
        
        resultSet_table.close();
        System.out.println("-----------------------------------------------------------------------");
    }
    
    /**
     * Test getTableTypes, configuration is available to select wanted data.
     *
     * @param conn                      The connection.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getTableTypes(Connection conn) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");
        System.out.println("Test get table types");
        
        // Add the default hard coded list of supported table types.
        List<String> defaultTableTypes = new ArrayList<String>();
        defaultTableTypes.add("EXTERNAL_TABLE");
        defaultTableTypes.add("MANAGED_TABLE");
        defaultTableTypes.add("TABLE");
        defaultTableTypes.add("VIEW");

        DatabaseMetaData md = conn.getMetaData();
        ResultSet resultSet_tableType = md.getTableTypes();
        List<String> tableTypes = new ArrayList<String>();
        while (resultSet_tableType.next())
        {
            tableTypes.add(resultSet_tableType.getString(1));
            System.out.println(resultSet_tableType.getString(1) + " | ");
        }
        resultSet_tableType.close();
        System.out.println("-----------------------------------------------------------------------");
        if(tableTypes.equals(defaultTableTypes))
        {
            System.out.println("The list of table types matches the Athena default list.");
        }
        else
        {
            System.out.println("The list of table types does not match the Athena default list.");
        }
        System.out.println("-----------------------------------------------------------------------");
    }

    /**
     * Test getTypeInfo, configuration is available to select wanted data.
     *
     * @param conn                      The connection.
     *
     * @throws SQLException             An <code>SQLException</code> will be thrown if there
     *                                  is an error.
     */
    protected static void getTypeInfo(Connection conn) throws SQLException
    {
        System.out.println("-----------------------------------------------------------------------");
        System.out.println("Test get type info");

        DatabaseMetaData md = conn.getMetaData();
        ResultSet resultSet_table = md.getTypeInfo();
        
        ResultSetMetaData rsmd = resultSet_table.getMetaData();
        printResultSetMetadata(rsmd);
        
        System.out.println("");
        printResultSet(resultSet_table);

        resultSet_table.close();
        System.out.println("-----------------------------------------------------------------------");
    }
    
    /**
     * Prints the result sets.
     * 
     * @param resultSet                 The resultset to print.
     * 
     * @throws SQLException 
     */
    private static void printResultSet(ResultSet resultSet) throws SQLException
    {
        int rowCount = 0;
        while (resultSet.next())
        {
            rowCount++;
            for (int j = 1; j <= resultSet.getMetaData().getColumnCount(); ++j)
            {
                System.out.print(resultSet.getString(j) + "|");
            }
            System.out.println("");
        }
        System.out.println("Rows retrieved: " + rowCount);
    }
    
    /**
     * Prints the metadata of the resultset.
     * 
     * @param resultSet                 The resultset to print.
     * 
     * @throws SQLException 
     */
    private static void printResultSetMetadata(ResultSetMetaData rsmd) throws SQLException
    {
        System.out.println("Column COUNT is: " + rsmd.getColumnCount());
        System.out.println("Column metadata: ");
        ArrayList<String> nameCache = new ArrayList<String>();
        for (int i = 1; i <= rsmd.getColumnCount(); ++i)
        {
            nameCache.add(rsmd.getColumnName(i));
            System.out.print(
                "Column Name: " + rsmd.getColumnName(i) + ", " + 
                "Display Size: " + rsmd.getColumnDisplaySize(i) + ", " +
                "Type Name: " + rsmd.getColumnTypeName(i) + ", " +
                "Type ID: " + rsmd.getColumnType(i) + ", " +
                "Precision: " + rsmd.getPrecision(i) + ", " +
                "Scale: " + rsmd.getScale(i) + ", " + 
                "CatalogName: " + rsmd.getCatalogName(i) + ", " +
                "SchemaName: " + rsmd.getSchemaName(i) + ", " +
                "TableName: " + rsmd.getTableName(i));
            System.out.println("");
        }
    }
}
