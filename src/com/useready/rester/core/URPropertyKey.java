// =================================================================================================
///  @file QJPropertyKey.java
///
///  QuickJson Connection Keys.
///
///  Copyright (C) 2015 Simba Technologies Incorporated.
// =================================================================================================

package com.useready.rester.core;

import com.simba.dsi.utilities.DSIPropertyKey;

/**
 * QuickJson connection setting keys.
 */
public class URPropertyKey extends DSIPropertyKey {
	/*
	 * Static variable(s)
	 * =========================================================================
	 * =
	 */

	/*
	 * Connection Properties
	 * -----------------------------------------------------------------------
	 */

	/**
	 * Key for Required property - paired value is the database file location.
	 */
	public static final String DBF = "DBF";

	/**
	 * Key for Optional property - paired value is the max string column length.
	 */
	public static final String DEFAULTMAXCOLUMNSIZE = "DEFAULTMAXCOLUMNSIZE";

	/**
	 * Key for Optional property - paired value is the language for the
	 * connection.
	 */
	public static final String LANGUAGE = "Language";

	/**
	 * Key for Optional property - paired value is the hybrid driver mode for
	 * the connection.
	 */
	public static final String MODE = "Mode";

	public static final String USER = "user";

	public static final String PASSWORD = "password";
	
	public static final String COB_DATE = "CoBDate";
	
	public static final String SERVER = "server";
	public static final String CONFIG = "config";
	
	public static final String TESTPROP="testProp";
	
	public static final String V_SHOWLATEST = "v-showlatest";
	public static final String V_ENVIROMENT = "v-enviroment";
	public static final String V_DOMAIN = "v-domain";
	public static final String AUTH = "authentication";
	public static final String TOKEN = "TOKEN";
	static final String PORT = "port";
//	public static final String COB_DATE = "CoBDate";
    

}
