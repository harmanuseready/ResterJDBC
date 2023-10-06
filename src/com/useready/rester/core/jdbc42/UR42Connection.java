// =================================================================================================
///  @file QJ42Connection.java
///
///  Definition of the class QJ42Connection.
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.core.jdbc42;

import com.simba.dsi.core.utilities.ClientInfoData;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ClientInfoException;
import com.simba.support.exceptions.ErrorException;
import com.useready.rester.core.URConnection;
import com.useready.rester.core.UREnvironment;
//import com.useready.rester.core.jdbc42.UR42Environment;
//import com.useready.rester.core.UREnvironment;

/**
 * Implementation of IConnection for JDBC42.
 */
public class UR42Connection extends URConnection
{
    /*
     * Constructor(s) ==============================================================================
     */

    /**
     * Constructor.
     *
     * @param environment               The parent environment.
     *
     * @throws ErrorException           If an error occurs.
     */
    public UR42Connection(UREnvironment environment) throws ErrorException
    {
        super(environment);
        loadClientInfoProperties();
    }

    /*
     * Method(s) ===================================================================================
     */

    /**
     * Set the client info property value to the database.
     *
     * @param propName                  
     * 				The client info property name.
     * @param propValue                 
     * 				The value for the given property.
     *
     * @throws ClientInfoException      
     * 				If an error occurs.
     */
    @Override
    public void setClientInfoProperty(String propName, String propValue)
        throws ClientInfoException
    {
        // Check that the property name is valid and store the new property values.
        super.setClientInfoProperty(propName, propValue);

        // TODO #20: Implements the wanted behaviour
        // Usually the driver stores the value specified in a suitable location in the database.
        // For example in a special register, session parameter, or system table column.
        LogUtilities.logInfo(
            String.format("Property %s has now the value %s", propName, propValue),
            m_log);
    }

    /*
     * Helper(s) ===================================================================================
     */

    /**
     * Initialize the valid client info properties list.
     *
     * @throws ErrorException
     * 				If an error occurs.
     */
    private void loadClientInfoProperties() throws ErrorException
    {
        // TODO #19: Define your custom client info properties.
        // Standard client info properties are Application_name, Client_user and client_hostname.
        // Other client info properties have to be defined here

        ClientInfoData fakeCustomCLientInfo = new ClientInfoData(
            URClientInfoPropertyKey.QJ_CUSTOM_CLIENT_INFO,
            25,
            "FakeCustomClientInfoForQuickJSON",
            "Just a fake client info property to show how to define them.");

        setClientInfoProperty(fakeCustomCLientInfo);
    }
}
