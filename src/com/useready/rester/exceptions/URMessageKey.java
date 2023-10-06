// =================================================================================================
/// @file QJMessageKey.java
///
/// Class specifying exception message keys.
///
/// Copyright (C) 2015 Simba Technologies Incorporated.
// =================================================================================================

package com.useready.rester.exceptions;

/**
 * These keys must match the messages.properties keys.
 */
public enum URMessageKey
{
    /*
     * Static variable(s) ==========================================================================
     */

    /**
     * Input string tokens: 0.
     */
    CONN_DEFAULT_PROP_ERR,

    /**
     * Input string tokens: 0.
     */
    DRIVER_DEFAULT_PROP_ERR,

    /**
     * Input string tokens: 0.
     */
    FILE_FORMAT_ERROR,

    /**
     * Input string tokens:
     * <ol>
     * <li>type.</li>
     * </ol>
     */
    INVALID_TYPE,

    /**
     * Input string tokens:
     * <ol>
     * <li>The path which is invalid.</li>
     * </ol>
     */
    INVALID_DBF_PATH,

    /**
     * Input string tokens: 0.
     */
    GENERIC_TABLE_LOADING_ERROR,

    /**
     * Input string tokens:
     * <ol>
     * <li>type.</li>
     * </ol>
     */
    UNKNOWN_JSON_TYPE,

    /**
     * Input string tokens: 0.
     */
    NUMERIC_OVERFLOW,

    /**
     * Input string tokens: 0.
     */
    INVALID_BLANK_COLUMN_NAME,

    /**
     * Input string tokens:
     * <ol>
     * <li>length 1.</li>
     * <li>length 2.</li>
     * </ol>
     */
    MAX_COLUMN_SIZE_VIOLATION,

    /**
     * Input string tokens:
     * <ol>
     * <li>format.</li>
     * </ol>
     */
    UNSUPPORTED_JSON_FORMAT,

    /**
     * Input string tokens: 0.
     */
    EMPTY_OBJECT,

    /**
     * Input string tokens: 0.
     */
    EMPTY_ARRAY,

    /**
     * Input string tokens: 0.
     */
    INCORRECT_TYPE,

    /**
     * Input string tokens: 0.
     */
    DATA_TYPE_MISMATCH
}
