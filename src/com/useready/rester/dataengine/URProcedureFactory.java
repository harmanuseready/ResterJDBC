// =================================================================================================
///  @file QJProcedureFactory.java
///
///  Definition of the Class QJProcedureFactory
///
///  Copyright (C) 2016 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.dataengine;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.simba.dsi.core.utilities.SqlType;
import com.simba.dsi.dataengine.utilities.ParameterType;
import com.simba.sqlengine.dsiext.dataengine.Identifier;
import com.simba.sqlengine.dsiext.dataengine.StoredProcedure;
import com.simba.support.ILogger;
import com.simba.support.IWarningListener;
import com.simba.support.exceptions.DiagState;
import com.simba.support.exceptions.ErrorException;

public class URProcedureFactory 
{
	/*
     * Static variable(s) ==========================================================================
     */
	
    /**
     * Default catalog for this set of stored procedures.
     */
    public static final String DEFAULT_CATALOG = 
    		URSingleParamProcedure.DEFAULT_CATALOG;
    
    /**
     * Default schema for this set of stored procedures.
     */
    public static final String DEFAULT_SCHEMA =
    		URSingleParamProcedure.DEFAULT_SCHEMA;
    
	/**
     * Input parameter name.
     */
    public static final String IN_PARAMETER_NAME = "in_param";

    /**
     * Output parameter name.
     */
    public static final String OUT_PARAMETER_NAME = "out_param";

    /**
     * in-out parameter name.
     */
    public static final String INOUT_PARAMETER_NAME = "in_out_param";

    /**
     * Return parameter name.
     */
    public static final String RETURN_PARAMETER_NAME = "return_param";
	
    
    
    final static Map<String, ParameterType> ParameterTypeMap;
    final static Map<ParameterType, String> ReverseParameterTypeMap;
    static
    {
        ParameterTypeMap = new HashMap<String, ParameterType>();
        ReverseParameterTypeMap = new HashMap<ParameterType, String>();

        ParameterTypeMap.put("In", ParameterType.INPUT);
        ReverseParameterTypeMap.put(ParameterType.INPUT, "In");
        ParameterTypeMap.put("InOut", ParameterType.INPUT_OUTPUT);
        ReverseParameterTypeMap.put(ParameterType.INPUT_OUTPUT, "InOut");
        ParameterTypeMap.put("Out", ParameterType.OUTPUT);
        ReverseParameterTypeMap.put(ParameterType.OUTPUT, "Out");
        ParameterTypeMap.put("Return", ParameterType.RETURN_VALUE);
        ReverseParameterTypeMap.put(ParameterType.RETURN_VALUE, "Return");
    }
    
    /*
     * Inner classes
     */
    
    private static class Info
    {
        Identifier m_id;
        ParameterType m_type;
        Integer m_sqlType;

        Info(String name,ParameterType type, Integer sqlType)
        {
            m_id = new Identifier(DEFAULT_CATALOG, DEFAULT_SCHEMA, name);
            m_type = type;
            m_sqlType = sqlType;
        }
    }
    
    /**
     * List of procedure, any procedure added should modify this list for the meta data to work fine.
     */
    private static HashMap<String, Info> s_ids;
    
    static
    {
    	s_ids = new HashMap<String, Info>();
    	
    	s_ids.put("sp_returnvaluetest", new Info("sp_returnvaluetest", ParameterType.RETURN_VALUE, Types.INTEGER));

        /**
         * Input parameter procedures
         */
        s_ids.put("in_AsciiStream_sp", new Info("in_AsciiStream_sp",
                ParameterType.INPUT,
                Types.VARCHAR));

        s_ids.put("in_BigDecimal_sp", new Info("in_BigDecimal_sp",
                ParameterType.INPUT,
                Types.NUMERIC));

        s_ids.put("in_BinaryStream_sp", new Info("in_BinaryStream_sp",
                ParameterType.INPUT,
                Types.LONGVARBINARY));

        s_ids.put("in_Blob_sp", new Info("in_Blob_sp",
                ParameterType.INPUT,
                Types.VARBINARY));

        s_ids.put("in_Boolean_sp", new Info("in_Boolean_sp",
                ParameterType.INPUT,
                Types.BIT));

        s_ids.put("in_Byte_sp", new Info("in_Byte_sp",
                ParameterType.INPUT,
                Types.TINYINT));

        s_ids.put("in_Bytes_sp", new Info("in_Bytes_sp",
                ParameterType.INPUT,
                Types.VARBINARY));

        s_ids.put("in_CharacterStream_sp", new Info("in_CharacterStream_sp",
                ParameterType.INPUT,
                Types.LONGVARCHAR));

        s_ids.put("in_Clob_sp", new Info("in_Clob_sp",
                ParameterType.INPUT,
                Types.VARCHAR));

        s_ids.put("in_Date_sp", new Info("in_Date_sp",
                ParameterType.INPUT,
                Types.DATE));

        s_ids.put("in_Double_sp", new Info("in_Double_sp",
                ParameterType.INPUT,
                Types.DOUBLE));

        s_ids.put("in_Float_sp", new Info("in_Float_sp",
                ParameterType.INPUT,
                Types.FLOAT));

        s_ids.put("in_Int_sp", new Info("in_Int_sp",
                ParameterType.INPUT,
                Types.INTEGER));

        s_ids.put("in_Long_sp", new Info("in_Long_sp",
                ParameterType.INPUT,
                Types.BIGINT));

        s_ids.put("in_Object_sp", new Info("in_Object_sp",
                ParameterType.INPUT,
                Types.INTEGER));

        s_ids.put("in_real_sp", new Info("in_real_sp",
                ParameterType.INPUT,
                Types.REAL));

        s_ids.put("in_Short_sp", new Info("in_Short_sp",
                ParameterType.INPUT,
                Types.SMALLINT));

        s_ids.put("in_RowId_sp", new Info("in_RowId_sp",
                ParameterType.INPUT,
                Types.VARCHAR));

        s_ids.put("in_Ref_sp", new Info("in_Ref_sp",
                ParameterType.INPUT,
                Types.VARCHAR));

        s_ids.put("in_Array_sp", new Info("in_Array_sp",
                ParameterType.INPUT,
                Types.VARCHAR));

        s_ids.put("in_sqlxml_sp", new Info("in_sqlxml_sp",
                ParameterType.INPUT,
                Types.VARCHAR));

        s_ids.put("in_String_sp", new Info("in_String_sp",
                ParameterType.INPUT,
                Types.VARCHAR));

        s_ids.put("in_Time_sp", new Info("in_Time_sp",
                ParameterType.INPUT,
                Types.TIME));

        s_ids.put("in_Time3_sp", new Info("in_Time3_sp",
                ParameterType.INPUT,
                Types.TIME));

        s_ids.put("in_Timestamp_sp", new Info("in_Timestamp_sp",
                ParameterType.INPUT,
                Types.TIMESTAMP));

        s_ids.put("in_UnicodeStream_sp", new Info("in_UnicodeStream_sp",
                ParameterType.INPUT,
                Types.LONGVARCHAR));

        s_ids.put("in_Url_sp", new Info("in_Url_sp",
                ParameterType.INPUT,
                Types.VARCHAR));

        s_ids.put("in_Guid_sp", new Info("in_Guid_sp",
                ParameterType.INPUT,
                SqlType.TYPE_SQL_GUID));

        /**
         * Output parameter procedures.
         */
        s_ids.put("out_asciistream_sp", new Info("out_asciistream_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_BigDecimal_sp", new Info("out_BigDecimal_sp",
                ParameterType.OUTPUT,
                Types.NUMERIC));

        s_ids.put("out_binarystream_sp", new Info("out_binarystream_sp",
                ParameterType.OUTPUT,
                Types.LONGVARBINARY));

        s_ids.put("out_Blob_sp", new Info("out_Blob_sp",
                ParameterType.OUTPUT,
                Types.VARBINARY));

        s_ids.put("out_Boolean_sp", new Info("out_Boolean_sp",
                ParameterType.OUTPUT,
                Types.BIT));

        s_ids.put("out_Byte_sp", new Info("out_Byte_sp",
                ParameterType.OUTPUT,
                Types.TINYINT));

        s_ids.put("out_Bytes_sp", new Info("out_Bytes_sp",
                ParameterType.OUTPUT,
                Types.VARBINARY));

        s_ids.put("out_CharacterStream_sp", new Info("out_CharacterStream_sp",
                ParameterType.OUTPUT,
                Types.LONGVARCHAR));

        s_ids.put("out_Clob_sp", new Info("out_Clob_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_Date_sp", new Info("out_Date_sp",
                ParameterType.OUTPUT,
                Types.DATE));

        s_ids.put("out_Double_sp", new Info("out_Double_sp",
                ParameterType.OUTPUT,
                Types.DOUBLE));

        s_ids.put("out_Float_sp", new Info("out_Float_sp",
                ParameterType.OUTPUT,
                Types.FLOAT));

        s_ids.put("out_Int_sp", new Info("out_Int_sp",
                ParameterType.OUTPUT,
                Types.INTEGER));

        s_ids.put("out_Long_sp", new Info("out_Long_sp",
                ParameterType.OUTPUT,
                Types.BIGINT));

        s_ids.put("out_Object_sp", new Info("out_Object_sp",
                ParameterType.OUTPUT,
                Types.INTEGER));

        s_ids.put("out_real_sp", new Info("out_real_sp",
                ParameterType.OUTPUT,
                Types.REAL));

        s_ids.put("out_Short_sp", new Info("out_Short_sp",
                ParameterType.OUTPUT,
                Types.SMALLINT));

        s_ids.put("out_RowId_sp", new Info("out_RowId_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_Ref_sp", new Info("out_Ref_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_Array_sp", new Info("out_Array_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_Sqlxml_sp", new Info("out_Sqlxml_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_String_sp", new Info("out_String_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_Time_sp", new Info("out_Time_sp",
                ParameterType.OUTPUT,
                Types.TIME));

        s_ids.put("out_Time3_sp", new Info("out_Time3_sp",
                ParameterType.OUTPUT,
                Types.TIME));

        s_ids.put("out_Timestamp_sp", new Info("out_Timestamp_sp",
                ParameterType.OUTPUT,
                Types.TIMESTAMP));

        s_ids.put("out_Url_sp", new Info("out_Url_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_Struct_sp", new Info("out_Struct_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_Datalink_sp", new Info("out_Datalink_sp",
                ParameterType.OUTPUT,
                Types.VARCHAR));

        s_ids.put("out_Guid_sp", new Info("out_Guid_sp",
                ParameterType.OUTPUT,
                SqlType.TYPE_SQL_GUID));

        /**
         * InOut parameters procedures.
         */
        s_ids.put("inout_asciistream_sp", new Info("inout_asciistream_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARCHAR));

        s_ids.put("inout_BigDecimal_sp", new Info("inout_BigDecimal_sp",
                ParameterType.INPUT_OUTPUT,
                Types.NUMERIC));

        s_ids.put("inout_binarystream_sp", new Info("inout_binarystream_sp",
                ParameterType.INPUT_OUTPUT,
                Types.LONGVARBINARY));

        s_ids.put("inout_Blob_sp", new Info("inout_Blob_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARBINARY));

        s_ids.put("inout_Boolean_sp", new Info("inout_Boolean_sp",
                ParameterType.INPUT_OUTPUT,
                Types.BIT));

        s_ids.put("inout_Byte_sp", new Info("inout_Byte_sp",
                ParameterType.INPUT_OUTPUT,
                Types.TINYINT));

        s_ids.put("inout_Bytes_sp", new Info("inout_Bytes_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARBINARY));

        s_ids.put("inout_CharacterStream_sp", new Info("inout_CharacterStream_sp",
                ParameterType.INPUT_OUTPUT,
                Types.LONGVARCHAR));

        s_ids.put("inout_Clob_sp", new Info("inout_Clob_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARCHAR));

        s_ids.put("inout_Date_sp", new Info("inout_Date_sp",
                ParameterType.INPUT_OUTPUT,
                Types.DATE));

        s_ids.put("inout_Double_sp", new Info("inout_Double_sp",
                ParameterType.INPUT_OUTPUT,
                Types.DOUBLE));

        s_ids.put("inout_Float_sp", new Info("inout_Float_sp",
                ParameterType.INPUT_OUTPUT,
                Types.FLOAT));

        s_ids.put("inout_Int_sp", new Info("inout_Int_sp",
                ParameterType.INPUT_OUTPUT,
                Types.INTEGER));

        s_ids.put("inout_Long_sp", new Info("inout_Long_sp",
                ParameterType.INPUT_OUTPUT,
                Types.BIGINT));

        s_ids.put("inout_Object_sp", new Info("inout_Object_sp",
                ParameterType.INPUT_OUTPUT,
                Types.INTEGER));

        s_ids.put("inout_real_sp", new Info("inout_real_sp",
                ParameterType.INPUT_OUTPUT,
                Types.REAL));

        s_ids.put("inout_Short_sp", new Info("inout_Short_sp",
                ParameterType.INPUT_OUTPUT,
                Types.SMALLINT));

        s_ids.put("inout_RowId_sp", new Info("inout_RowId_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARCHAR));

        s_ids.put("inout_Ref_sp", new Info("inout_Ref_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARCHAR));

        s_ids.put("inout_Array_sp", new Info("inout_Array_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARCHAR));

        s_ids.put("inout_sqlxml_sp", new Info("inout_sqlxml_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARCHAR));

        s_ids.put("inout_String_sp", new Info("inout_String_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARCHAR));

        s_ids.put("inout_Time_sp", new Info("inout_Time_sp",
                ParameterType.INPUT_OUTPUT,
                Types.TIME));

        s_ids.put("inout_Time3_sp", new Info("inout_Time3_sp",
                ParameterType.INPUT_OUTPUT,
                Types.TIME));

        s_ids.put("inout_Timestamp_sp", new Info("inout_Timestamp_sp",
                ParameterType.INPUT_OUTPUT,
                Types.TIMESTAMP));

        s_ids.put("inout_Url_sp", new Info("inout_Url_sp",
                ParameterType.INPUT_OUTPUT,
                Types.VARCHAR));

        s_ids.put("inout_Guid_sp", new Info("inout_Guid_sp",
                ParameterType.INPUT_OUTPUT,
                SqlType.TYPE_SQL_GUID));
    }

    
    
    /*
     * Static Methods ==============================================================================
     */
    
    static StoredProcedure open(String catalogName, String schemaName, String procName, 
    		ILogger in_logger, IWarningListener in_Wlistener) 
    		throws ErrorException
    {
    	if(s_ids.containsKey(procName) &&
    			(null == catalogName || catalogName.isEmpty() || catalogName == DEFAULT_CATALOG) &&
    			(null == schemaName || schemaName.isEmpty() || schemaName == DEFAULT_SCHEMA))
    	{
    		Info info = s_ids.get(procName);
    		return new URSingleParamProcedure(
                    procName,
                    info.m_type,
                    info.m_sqlType,
                    in_logger,
                    in_Wlistener);
    	}
    	else
    	{
    		throw new ErrorException(
    				DiagState.DIAG_GENERAL_ERROR,
    				"Procedure does not exist.",
    				0);
    	}
    }
    
 
    /**
     * Get a list of stored procedure identifiers for metadata purposes.
     * @return List of supported stored procedures.
     */
    public static Iterator<Identifier> getListofProcedures()
    {
        ArrayList<Identifier> list = new ArrayList<Identifier>();
        for (Map.Entry<String, Info> entry : s_ids.entrySet())
        {
            list.add(entry.getValue().m_id);
        }
        return list.iterator();
    }
    
   

}
