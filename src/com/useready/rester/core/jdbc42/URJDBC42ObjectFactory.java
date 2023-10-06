// =================================================================================================
/// @file QJJDBC42ObjectFactory.java
///
/// Implementation of the QJJDBC42ObjectFactory.
///
/// Copyright (C) 2015 Simba Technologies Incorporated.
// =================================================================================================

package com.useready.rester.core.jdbc42;

import java.sql.SQLException;

import com.simba.dsi.core.interfaces.IConnection;
import com.simba.dsi.core.interfaces.IStatement;
import com.simba.jdbc.common.SCallableStatement;
import com.simba.jdbc.common.SConnection;
import com.simba.jdbc.common.SDatabaseMetaData;
import com.simba.jdbc.common.SPreparedStatement;
import com.simba.jdbc.common.SStatement;
import com.simba.jdbc.jdbc42.JDBC42ObjectFactory;
import com.simba.support.ILogger;

/**
 * Sample implementation of JDBCObjectFactory.
 */
public class URJDBC42ObjectFactory extends JDBC42ObjectFactory
{
    /*
     * Constructor =================================================================================
     */

    /**
     * Constructor.
     */
    protected URJDBC42ObjectFactory()
    {
        ; // Do nothing.
    }

    /*
     * Method(s) ===================================================================================
     */

    /**
     * Create a new instance of a CallableStatement.
     *
     * @param sql               The SQL statement that this CallableStatement represents.
     * @param dsiStatement      The JDSI statement object that this Statement is based on.
     * @param parentConnection  The Connection object that created this Statement.
     * @param concurrency       Either ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE.
     *
     * @throws SQLException if a database access error occurs.
     */
    @Override
    protected SCallableStatement createCallableStatement(
        String sql,
        IStatement dsiStatement,
        SConnection parentConnection,
        int concurrency)
        throws SQLException
    {
        // This is where a subclassed object would be returned.
        return super.createCallableStatement(sql, dsiStatement, parentConnection, concurrency);
    }

    /**
     * Create a new instance of a Connection.
     *
     * @param dsiConnection     The IConnection object created by the ConnectionFactory, which this
     *                          object is based upon.
     * @param url               The URL used to create this connection to the database.
     *
     * @throws SQLException if a database access error occurs.
     */
    @Override
    protected SConnection createConnection(IConnection dsiConnection, String url)
        throws SQLException
    {
        // This is where a subclassed object would be returned.
        return super.createConnection(dsiConnection, url);
    }

    /**
     * Create a new instance of a DatabaseMetaData.
     *
     * @param conn              The Connection object that created this MetaData.
     * @param logger            The logger to use to log messages.
     *
     * @throws SQLException if a database access error occurs.
     */
    @Override
    protected SDatabaseMetaData createDatabaseMetaData(SConnection conn, ILogger logger)
        throws SQLException
    {
        // This is where a subclassed object would be returned.
        return super.createDatabaseMetaData(conn, logger);
    }

    /**
     * Create a new instance of a PreparedStatement.
     *
     * @param sql               The SQL statement that this PreparedStatement represents.
     * @param dsiStatement      The JDSI statement object that this Statement is based on.
     * @param parentConnection  The Connection object that created this Statement.
     * @param concurrency       Either ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE.
     *
     * @throws SQLException if a database access error occurs.
     */
    @Override
    protected SPreparedStatement createPreparedStatement(
        String sql,
        IStatement dsiStatement,
        SConnection parentConnection,
        int concurrency)
        throws SQLException
    {
        // This is where a subclassed object would be returned.
        return super.createPreparedStatement(sql, dsiStatement, parentConnection, concurrency);
    }

    /**
     * Create a new instance of a Statement.
     *
     * @param dsiStatement      The JDSI statement object that this Statement is based on.
     * @param parentConnection  The Connection object that created this Statement.
     * @param concurrency       Either ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE.
     *
     * @throws SQLException if a database access error occurs.
     */
    @Override
    protected SStatement createStatement(
        IStatement dsiStatement,
        SConnection parentConnection,
        int concurrency)
        throws SQLException
    {
        // This is where a subclassed object would be returned.
        return super.createStatement(dsiStatement, parentConnection, concurrency);
    }
}
