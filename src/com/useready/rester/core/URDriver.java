// =================================================================================================
///  @file QJDriver.java
///
///  Definition of the Class QJDriver.
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.core;

import com.simba.dsi.core.impl.DSIDriver;
import com.simba.dsi.core.impl.DSILogger;
import com.simba.dsi.core.interfaces.IEnvironment;
import com.simba.dsi.core.utilities.ConnPropertyKey;
import com.simba.dsi.core.utilities.DriverPropertyKey;
import com.simba.dsi.core.utilities.Variant;
import com.simba.sqlengine.SQLEngineGenericContext;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.ErrorException;
import com.simba.support.exceptions.ExceptionBuilder;
import com.simba.support.exceptions.ExceptionUtilities;
import com.useready.rester.QuickJson;
import com.useready.rester.exceptions.URMessageKey;
import com.useready.rester.core.jdbc42.*;

import java.text.MessageFormat;

/**
 * Implementation of IDriver. Extends DSIDriver which initializes and manages driver properties.
 */
public class URDriver extends DSIDriver
{
    /*
     * Static initializations ======================================================================
     */

    /**
     * The driver name.
     */
    // TODO #1: Set the driver name.
    public static final String DRIVER_NAME = "usereadyjdbc";

    /**
     * The major version of this driver.
     */
    public static final int DRIVER_MAJOR_VERSION = 1;

    /**
     * The minor version of this driver.
     */
    public static final int DRIVER_MINOR_VERSION = 0;

    /**
     * The file name to use for logging.
     */
    public static final String LOG_NAME = DRIVER_NAME + "_driver";

    // TODO #10: Update Messages properties file.
    /**
     * The base name of the properties file to load for the resource bundle, excluding the class
     * path.
     */
    public static final String RESOURCE_NAME = "messages";

    /**
     * The exception builder instance to use for QuickJson.
     */
    public static ExceptionBuilder s_QJMessages = new ExceptionBuilder(QuickJson.QJ_ERROR);

    /*
     * Instance variable(s) ========================================================================
     */

    /**
     * Logger.
     */
    // TODO #4: Set the driver-wide logging details.
    private DSILogger m_log = new DSILogger(LOG_NAME);

    
    /*
     * Constructor(s) ==============================================================================
     */

    /**
     * Constructs a ULDriver overriding the default properties of the parent DSIDriver.
     *
     * @throws ErrorException           If an error occurs.
     */
    public URDriver() throws ErrorException
    {
        // TODO #11: Register your error messages for handling by DSIMessageSource.
        // Register the error messages for handling by DSIMessageSource.
    	System.out.println(ConnPropertyKey.DSI_SERVER_NAME);
        StringBuilder messagesFileName =
            new StringBuilder(ExceptionUtilities.getPackageName(this.getClass()));
        messagesFileName.append(".");
        messagesFileName.append(RESOURCE_NAME);
        m_msgSrc.registerMessages(
            messagesFileName.toString(),
            QuickJson.QJ_ERROR,
            QuickJson.QJ_COMPONENT_NAME);
        
        // Configure the JSQLEngine to use the same message source as the driver
        SQLEngineGenericContext.setDefaultMsgSource(m_msgSrc);

        // TODO #12: Set the vendor name, which will be prepended to error messages.
        /*
        NOTE: We do not set the vendor name here on purpose, because the default vendor name is
        'Simba'. The code below shows how to override the default vendor name.

        m_msgSrc.setVendorName("vendorName");
         */
        m_msgSrc.setVendorName("usereadyJDBCDriver");
        setDefaultProperties();
    }

    /*
     * Method(s) ===================================================================================
     */

    /**
     * Create the environment.
     *
     * @return the new environment.
     *
     * @throws ErrorException           If an error occurs.
     */
    public IEnvironment createEnvironment() throws ErrorException
    {
        LogUtilities.logFunctionEntrance(m_log);
        return new UREnvironment(this);
    }

    /**
     * Gets the driver-wide DSILogger for logging.
     *
     * @return Log for driver-wide logging
     */
    public DSILogger getDriverLog()
    {
        return m_log;
    }

    /**
     * Unloads the driver and releases any resources held by it.
     */
    public void unload()
    {
        m_log = null;
    }

    /**
     * Loads the default property values for the driver.
     *
     * @throws ErrorException           If an error occurs.
     */
    private void setDefaultProperties() throws ErrorException
    {
        // TODO #2: Set the driver properties. This allows you to override any of the default values
        // that are set by DSIDriver.
        try
        {
            setProperty(
                DriverPropertyKey.DSI_DRIVER_NAME,
                new Variant(Variant.TYPE_WSTRING, DRIVER_NAME));

            String driverVer = MessageFormat.format(
                "{0,number,00}.{1,number,00}.0000",
                DRIVER_MAJOR_VERSION,
                DRIVER_MINOR_VERSION);
            setProperty(
                DriverPropertyKey.DSI_DRIVER_VER,
                new Variant(Variant.TYPE_WSTRING, driverVer));
        }
        catch (Throwable e)
        {
            // This should never happen.
            throw s_QJMessages.createGeneralException(URMessageKey.DRIVER_DEFAULT_PROP_ERR.name());
        }
    }
}
