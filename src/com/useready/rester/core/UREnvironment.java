// =================================================================================================
///  @file QJEnvironment.java
///
///  Definition of the class QJEnvironment.
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.core;

import com.simba.dsi.core.impl.DSIEnvironment;
import com.simba.dsi.core.interfaces.IConnection;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ErrorException;

/**
 * Implementation of IEnvironment. Extends DSIEnvironment which initializes and manages environment
 * properties.
 */
public class UREnvironment extends DSIEnvironment
{
    /*
     * Constructor(s) ==============================================================================
     */

    /**
     * Constructor.
     *
     * @param driver                    The parent driver of this environment.
     *
     * @throws ErrorException           If an error occurs.
     */
    protected UREnvironment(URDriver driver) throws ErrorException
    {
        super(driver);
        LogUtilities.logFunctionEntrance(driver.getDriverLog(), driver);
    }

    /*
     * Method(s) ===================================================================================
     */

    /**
     * Closes the environment and releases any resources held by it.
     */
    public void close()
    {
        LogUtilities.logFunctionEntrance(getLog());
    }

    /**
     * Factory method for creating IConnections.
     *
     * @return IConnection
     * @throws ErrorException
     *             if an error occurs.
     * @see IConnection
     */
    public IConnection createConnection() throws ErrorException
    {
        LogUtilities.logFunctionEntrance(getLog());
        return new URConnection(this);
    }
}
