// =================================================================================================
///  @file QJJDBC42Driver.java
///
///  Definition of the class QJJDBC42Driver.
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.core.jdbc42;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.simba.jdbc.common.AbstractDriver;
import com.simba.jdbc.common.JDBCObjectFactory;
import com.simba.jdbc.jdbc42.JDBC42AbstractDriver;
import com.useready.rester.core.URCoreUtils;
import com.useready.rester.core.URDriver;
import com.useready.rester.core.jdbc42.URJDBC42ObjectFactory;

/**
 * A concrete implementation of the Driver interface.
 */
public class URJDBC42Driver extends JDBC42AbstractDriver {
	static {
		try {
			// Initialize the driver by registering it with all of the
			// appropriate classes.
			AbstractDriver.initialize(new URJDBC42Driver(), URDriver.class.getName());
			AbstractDriver.setErrorMessageComponentName("JavaRestConnector");	
			// TODO #15: Set the JDBC Component Name for error messages
			// originating from the JDBC
			// layer.
			/*
			 * NOTE: We do not set the component name here on purpose, because
			 * the default component name is 'JDBC Driver'. The code below shows
			 * how to override the default component name.
			 * 
			  AbstractDriver.setErrorMessageComponentName("JavaQuickJSON");
			 */
		} catch (Exception e) {
			System.err.println(e.getLocalizedMessage());
		}
	}
	/*
     * Main Method ==============================================================================
     */
    // TODO #11: Parse properties for REST Service and set the appropriate methods here. 
    public static void main(String[] args) {
    	System.out.println("Driver for REST Service successfully loaded.");
//    	 String query = "SELECT CAST (EXTRACT(YEAR FROM \"Fields\".\"null.SubmitDate\") AS INTEGER) AS \"yr:null.SubmitDate:ok\" FROM \"CPRT_EqVolSlidePGArms 372326777 \".\"Fields\" \"Fields\" GROUP BY 1";

         // Define a regular expression pattern to match column names and aliases
//         String pattern = "AS\\s+\"(.*?)\"";
//         Pattern regex = Pattern.compile(pattern);
//
//         Matcher matcher = regex.matcher(query);
//
//         // Find and print column names and their aliases that match the pattern
//         while (matcher.find()) {
//             String columnName = matcher.group(1);
//           //  String alias = matcher.group(2);
//             String[] parts = columnName.split(":");
//             
//             if (parts.length >= 2) {
//                  columnName = parts[1];
//
//                 System.out.println("Column Name: " + columnName);
//             } else {
//                 System.out.println("Column name not found.");
//             }
//             System.out.println("Column Name: " + columnName);
//           //  System.out.println("Alias: " + alias);
//         }
     
    }
    
	/*
	 * Method(s)
	 * =========================================================================
	 * ==========
	 */

	/**
	 * Create the factory class used to create JDBC connection and statement
	 * objects.
	 *
	 * This will allow for sub-classing of these objects for addition of custom
	 * extensions if desired.
	 *
	 * Note that this will need to be overridden in the DSI implementation of
	 * both AbstractDriver and AbstractDataSource.
	 *
	 * @return A new instance of a JDBCObjectFactory.
	 */
	@Override
	protected JDBCObjectFactory createJDBCObjectFactory() {
		return new URJDBC42ObjectFactory();
	}

	/**
	 * Retrieve the subprotocol section of the JDBC URL used to connect to this
	 * driver. A full URL might look like
	 * &lt;jdbc&gt;:&lt;subprotocol&gt;:&lt;subname&gt;.
	 *
	 * @return A String representing the subprotocol. Should not be null.
	 */
	@Override
	protected String getSubProtocol() {
		// TODO #17: Set the subprotocol to which this driver will respond.
		return "useready";
	}

	/**
	 * Parse the subname section of the JDBC URL used to connect to this driver.
	 * Return true if this is an acceptable subname for this driver, otherwise
	 * return false. A full URL might look like
	 * &lt;jdbc&gt;:&lt;subprotocol&gt;:&lt;subname&gt;. The subname section
	 * might contain the DSN name, a port, or other information.
	 * <p>
	 * Expected format: "//host[;key=value]" where [] indicate optional values
	 *
	 * @param subname
	 *            The subname section of the given URL.
	 * @param properties
	 *            The properties to add the parsed information to.
	 * @return true if subname matches for this driver; false otherwise.
	 */
	@Override
	protected boolean parseSubName(String subname, Properties properties) {
		return URCoreUtils.parseSubName(subname, properties);
	}
}
