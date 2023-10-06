// =================================================================================================
///  @file QJJDBC42DataSource.java
///
///  Definition of the class QJJDBC42DataSource.
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.core.jdbc42;

import java.util.Properties;

import com.simba.jdbc.common.AbstractDataSource;
import com.simba.jdbc.jdbc42.JDBC42AbstractDataSource;
import com.simba.jdbc.jdbc42.JDBC42ObjectFactory;
import com.useready.rester.core.URCoreUtils;
import com.useready.rester.core.URDriver;
import com.useready.rester.core.URPropertyKey;

/**
 * Extension of a basic implementation of the DataSource interface.
 *
 * @see AbstractDataSource
 */
public class URJDBC42DataSource extends JDBC42AbstractDataSource
{
    static
    {
        // Initialize the data source by registering it with all of the appropriate classes.
        AbstractDataSource.initialize(URDriver.class.getName());

        // TODO #16: Set the JDBC Component Name for error messages originating from the JDBC layer.
        /*
        NOTE: We do not set the component name here on purpose, because the default component
        name is 'JDBC Driver'. The code below shows how to override the default component name.

        AbstractDriver.setErrorMessageComponentName("JavaQuickJSON");
        */
    }

    /*
     * Instance variable(s) ========================================================================
     */

    /**
     * The language to use.
     * <p>
     * Optional.
     * <p>
     * Example: EN_US
     * <p>
     * This example property would be used to change the language of the connection.
     */
    private String m_language = null;

    /*
     * Method(s) ===================================================================================
     */

    /**
     * @see URJDBC42DataSource#m_language
     * @return The language to use. Set by {@link URJDBC42DataSource#setLanguage(String)}.
     */
    public String getLanguage()
    {
        return m_language;
    }

    /**
     * Sets the language to use.
     *
     * @see URJDBC42DataSource#m_language
     * @param language
     *            The language to use.
     */
    public void setLanguage(String language)
    {
        m_language = language;
    }

    /**
     * Create the factory class used to create JDBC connection and statement objects.
     *
     * This will allow for sub-classing of these objects for addition of custom extensions if
     * desired.
     *
     * Note that this will need to be overridden in the DSI implementation of both AbstractDriver
     * and AbstractDataSource.
     *
     * @return A new instance of a JDBCObjectFactory.
     */
    @Override
    protected JDBC42ObjectFactory createJDBCObjectFactory()
    {
        return new URJDBC42ObjectFactory();
    }

    /**
     * Generates the properties for a connection based on the information currently held in the
     * class.
     *
     * @return A list of arbitrary string tag/value pairs as connection arguments.
     */
    @Override
    protected Properties getProperties()
    {
        Properties props = super.getProperties();

        if (null != m_language)
        {
            // Insert the language if it's been set.
            props.put(URPropertyKey.LANGUAGE, m_language);
        }

        return props;
    }

    /**
     * Retrieve the subprotocol section of the JDBC URL used to connect to this driver.
     * <p>
     * A full URL might look like &lt;jdbc&gt;:&lt;subprotocol&gt;:&lt;subname&gt;.
     *
     * @return A String representing the subprotocol. Should not be null.
     */
    @Override
    protected String getSubProtocol()
    {
        // TODO #18: Set the subprotocol to which this driver will respond.
        return "simba";
    }

    /**
     * Parse the subname section of the JDBC URL used to connect to this data source. Return true if
     * this is an acceptable subname for this data source, otherwise return false. A full URL might
     * look like &lt;jdbc&gt;:&lt;subprotocol&gt;:&lt;subname&gt;. The subname section might contain
     * the DSN name, a port, or other information.
     * <p>
     * Expected format: "//host[;key=value]" where [] indicate optional values
     *
     * @param subname
     *            The subname section of the given URL.
     * @param properties
     *            The properties to add the parsed information to.
     * @return true if subname matches for this data source; false otherwise.
     */
    @Override
    protected boolean parseSubName(String subname, Properties properties)
    {
        return URCoreUtils.parseSubName(subname, properties);
    }
}
