// =================================================================================================
///  @file QJ42Environment.java
///
///  Definition of the class QJ42Environment.
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.core.jdbc42;

import com.simba.dsi.core.interfaces.IConnection;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ErrorException;
import com.useready.rester.core.URDriver;
import com.useready.rester.core.UREnvironment;

/**
 * Implementation of IEnvironment.
 */
public class UR42Environment extends UREnvironment
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
    UR42Environment(URDriver driver) throws ErrorException
    {
        super(driver);
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
     * @return the new connection.
     *
     * @throws ErrorException           If an error occurs.
     */
    public IConnection createConnection() throws ErrorException
    {
        LogUtilities.logFunctionEntrance(getLog());
        return new UR42Connection(this);
    }
}
