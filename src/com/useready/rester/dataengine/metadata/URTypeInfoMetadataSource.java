// =================================================================================================
///  @file QJTypeInfoMetadataSource.java
///
///  Definition of the Class QJTypeInfoMetadataSource
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.dataengine.metadata;

import java.sql.Types;

import com.simba.dsi.core.utilities.SqlType;
import com.simba.dsi.exceptions.IncorrectTypeException;
import com.simba.dsi.exceptions.InvalidOperationException;
import com.simba.sqlengine.dsiext.dataengine.metadata.DSIExtTypeInfoMetadataSource;
import com.simba.support.ILogger;
import com.simba.support.exceptions.ErrorException;

/**
 * QuickJson sample metadata table for types supported by the DSI implementation.
 */
public class URTypeInfoMetadataSource extends DSIExtTypeInfoMetadataSource
{
    /**
     * Constructor
     *
     * @param logger 	The logger to use.
     * @param isODBC2	Whether we're running in the context of an ODBC 2.x driver.
     * @throws ErrorException If one of our type aliases is rejected.
     */
    public URTypeInfoMetadataSource(ILogger logger, boolean isODBC2) throws ErrorException
    {
        super(logger, isODBC2);
        try
        {
            SqlTypeInfo info;
            
            // Create some typename aliases. These can be used in the CAST function.
            info = createSqlTypeInfo(Types.BIT);
            info.m_typeName = "JSON_BOOLEAN";
            addUserDataTypeInfo(info);
            
             info = createSqlTypeInfo(Types.NUMERIC);
            info.m_typeName = "JSON_NUMERIC";
            addUserDataTypeInfo(info);
            
            info = createSqlTypeInfo(Types.DOUBLE);
            info.m_typeName = "JSON_DOUBLE";
            addUserDataTypeInfo(info);
            
            info = createSqlTypeInfo(Types.BIGINT);
            info.m_typeName = "JSON_BIGINT";
            addUserDataTypeInfo(info);
            
            info = createSqlTypeInfo(SqlType.TYPE_SQL_WVARCHAR);
            info.m_typeName = "JSON_STRING";
            addUserDataTypeInfo(info);
        }
        catch (IncorrectTypeException typeException)
        {
            // Shouldn't happen. Convert the typeException to a runtime exception
            RuntimeException e = new InvalidOperationException();
            e.initCause(typeException);
            throw e;
        }
    }

    @Override
    protected TypePrepared prepareType(SqlTypeInfo sqlTypeInfo)
    {
        switch (sqlTypeInfo.m_sqlType)
        {
            case Types.BIT:
            case Types.NUMERIC:
            case Types.DOUBLE:
            case Types.BIGINT:
            case SqlType.TYPE_SQL_WVARCHAR:
            {
                // These types are supported. Leave the type settings as default. Note that this is
                // where changes would be done if the data-source settings for a type were different
                // than the default settings.
                return TypePrepared.DONE;
            }
            default:
            {
                // All the other types are not supported.
                return TypePrepared.NOT_SUPPORTED;
            }
        }
    }
}
