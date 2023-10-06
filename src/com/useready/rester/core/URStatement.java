// =================================================================================================
///  @file QJStatement.java
///
///  Definition of the Class QJStatement
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.core;

import com.simba.dsi.core.impl.DSIStatement;
import com.simba.dsi.dataengine.interfaces.IDataEngine;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ErrorException;
import com.useready.rester.dataengine.URDataEngine;

/**
 * Extension of DSIStatement.
 *
 * @see DSIStatement
 */
public class URStatement extends DSIStatement
{
    /*
     * Constructor(s) ==============================================================================
     */

    /**
     * Constructor.
     *
     * @param connection
     *            The parent connection.
     */
    URStatement(URConnection connection) throws ErrorException
    {
        super(connection);
        LogUtilities.logFunctionEntrance(getLog(), connection);
    }

    /*
     * Method(s) ===================================================================================
     */

    /**
     * Closes the statement and releases any resources held by it.
     */
    public void close()
    {
        LogUtilities.logFunctionEntrance(getLog());
    }

    /**
     * @return A new IDataEngine that can be used to execute queries and harvest metadata, in the
     * context of this IStatement.
     *
     * @throws ErrorException
     *              If error occurs.
     */
    public IDataEngine createDataEngine() throws ErrorException
    {
        LogUtilities.logFunctionEntrance(getLog());
        return new URDataEngine(this);
    }
}
