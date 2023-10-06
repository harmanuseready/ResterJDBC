// =================================================================================================
///  @file QJOneParamProcedure.java
///
///  Definition of the Class QJOneParamProcedure
///
///  Copyright (C) 2016 Simba Technologies Incorporated
// =================================================================================================


package com.useready.rester.dataengine;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.UUID;

import com.simba.dsi.core.utilities.SqlType;
import com.simba.dsi.dataengine.impl.DSISimpleRowCountResult;
import com.simba.dsi.dataengine.interfaces.IColumn;
import com.simba.dsi.dataengine.interfaces.IResultSet;
import com.simba.dsi.dataengine.utilities.*;
import com.simba.sqlengine.dsiext.dataengine.ProcedureParameterMetadata;
import com.simba.sqlengine.dsiext.dataengine.ProcedureParameterValue;
import com.simba.sqlengine.dsiext.dataengine.StoredProcedure;
import com.simba.support.ILogger;
import com.simba.support.IWarningListener;
import com.simba.support.LogUtilities;
import com.simba.support.exceptions.DiagState;
import com.simba.support.exceptions.ErrorException;


/**
 * @brief XM sample stored procedure that has only one parameter. Used in testing.
 */

public class URSingleParamProcedure extends StoredProcedure
{

    /*
     * Static variable(s)  =========================================================================
     */
    /**
     * The default string value to return.
     */
    private static final String DEFAULT_STRING = "DEFAULT_OUTPUT_VALUE";

    /**
     * The default numeric value to return.
     */
    private static final int DEFAULT_NUMERIC = 7;
    
    /**
     * Default catalog for this set of stored procedures.
     */
    public static final String DEFAULT_CATALOG = "2022-02-24";
    
    /**
     * Default schema for this set of stored procedures.
     */
    public static final String DEFAULT_SCHEMA = "PROTIIDE OUTPUT";

    /*
     * Instance variable(s) ========================================================================
     */

    /**
     * The logger to use for logging.
     */
    private final ILogger m_logger;

    /**
     * Warning listener to pass the conversion warnings to.
     */
    private IWarningListener m_warningListener;

    /**
     * The parameter type of the procedure
     */
    private ParameterType m_paramType;

    /**
     * The Sql type of the parameter.
     */
    private int m_sqlType;

    /**
     * The list of parameters.
     */
    private List<ProcedureParameterMetadata> m_paramMeta;

    /**
     * The precision to use for the parameter, if null, use the default for the type.
     */
    private Integer m_overriddenPrecision = null;

    /*
     * Constructor(s) ==============================================================================
     */

    /**
     * Constructs XMSingleParamProcedure.
     * This procedure has a different catalog and schema that other procedures.
     *
     * @throws ErrorException
     */
    public URSingleParamProcedure(String name,
                                  ParameterType paramType,
                                  int sqlType,
                                  ILogger logger,
                                  IWarningListener warningListener)
    {
        super(
                DEFAULT_CATALOG,
                DEFAULT_SCHEMA,
                name);
        m_logger = logger;
        m_warningListener = warningListener;
        m_paramType = paramType;
        m_sqlType = sqlType;
    }

    /*
     * Method(s) ===================================================================================
     */
    public URSingleParamProcedure overridePrecision(int overridenPrecision)
    {
        m_overriddenPrecision = overridenPrecision;

        if (null != m_overriddenPrecision)
        {
            m_paramMeta.get(0).getTypeMetadata().setPrecision((short)(int)m_overriddenPrecision);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(ArrayList<ProcedureParameterValue> parameters) throws ErrorException
    {
        LogUtilities.logFunctionEntrance(m_logger, parameters);

        assert (null != parameters);
        Object inValue = null;

        for (int i = 0; i < parameters.size(); i++)
        {
            if ((ParameterType.INPUT == m_paramType) ||
                    (ParameterType.INPUT_OUTPUT == m_paramType))
            {
                if (!parameters.get(i).isNull())
                {
                    inValue = parameters.get(i).getData().getObject();
                }
            }
            if ((ParameterType.OUTPUT == m_paramType) ||
                    (ParameterType.INPUT_OUTPUT == m_paramType) ||
                    (ParameterType.RETURN_VALUE == m_paramType))
            {
            	setValue(parameters.get(i).getData(), inValue);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ProcedureParameterMetadata> getParameters() throws ErrorException
    {
       
        if (m_paramMeta == null)
        {
            ProcedureParameterMetadata paramMeta = new ProcedureParameterMetadata(
                    0,
                    m_paramType,
                    m_sqlType,
                    false);
            if (paramMeta.getTypeMetadata().getType() == Types.TIME) {
                paramMeta.getTypeMetadata().setPrecision((short) 3);
            }

            if ((m_sqlType == Types.DECIMAL) ||
                (m_sqlType == Types.NUMERIC))
            {
                paramMeta.getTypeMetadata().setScale((short) 5);
            }
            else if (paramMeta.getTypeMetadata().isCharacterOrBinaryType())
            {
                paramMeta.setColumnLength(1024);
            }

            switch (m_paramType)
            {
                case INPUT:
                {
                    paramMeta.setName(URProcedureFactory.IN_PARAMETER_NAME);
                    break;
                }

                case OUTPUT:
                {
                    paramMeta.setName(URProcedureFactory.OUT_PARAMETER_NAME);
                    break;
                }

                case INPUT_OUTPUT:
                {
                    paramMeta.setName(URProcedureFactory.INOUT_PARAMETER_NAME);
                    break;
                }

                case RETURN_VALUE:
                {
                    paramMeta.setName(URProcedureFactory.RETURN_PARAMETER_NAME);
                    break;
                }
			
			    default:
			    {
			    	throw new ErrorException(
			                DiagState.DIAG_GENERAL_ERROR,
			                "Parameter Type Unknown",
			                0);
			    }
            }

            m_paramMeta = new ArrayList<ProcedureParameterMetadata>();
            m_paramMeta.add(paramMeta);
        }
        return m_paramMeta;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExecutionResults getResults() throws ErrorException
    {
        // No results
        ExecutionResults results = new ExecutionResults();
        results.addRowCountResult(new DSISimpleRowCountResult(IResultSet.ROW_COUNT_UNKNOWN));
        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<IColumn> getCurrentResultMetadata()
    {
        // No results metadata. Return empty list
        return new ArrayList<IColumn>();
    }

    /**
     * Get the flag indicating if the stored procedure has return value
     */
    public boolean hasReturnValue()
    {
        return (ParameterType.RETURN_VALUE == m_paramType);
    }

    /*
     * Helper(s) ===================================================================================
     */
    /**
     * Generate a default date.
     *
     * @return the generated date.
     */
    private Date defaultDate()
    {
        return new Date(GregorianCalendar.getInstance().getTimeInMillis());
    }
    
    /**
     * Set the output parameter value.
     *
     * @param data                      The data to set.
     * @param value                     The value to use for the parameter.
     */
    private void setValue(DataWrapper data, Object value)
    {
        // Perform change on input value or set the default value if there is no input
        if (Types.BIGINT == m_sqlType)
        {
            BigInteger inValue = (BigInteger)
                ((value == null) ? BigInteger.valueOf(DEFAULT_NUMERIC) : value);

            data.setBigInt(inValue);
        }
        else if ((Types.BIT == m_sqlType) ||
                 (Types.BOOLEAN == m_sqlType))
        {
            Boolean inValue = (Boolean) ((value == null) ? true :  value);

            data.setBit(inValue);
        }
        else if (Types.DATE == m_sqlType)
        {
            Date inValue = (Date) ((value == null) ? defaultDate() : value);

            data.setDate(inValue);
        }
        else if ((Types.DECIMAL == m_sqlType) ||
                 (Types.NUMERIC == m_sqlType))
        {
            BigDecimal inValue =
                (BigDecimal) ((value == null) ? BigDecimal.valueOf(DEFAULT_NUMERIC) : value);

            data.setDecimal(inValue);
        }
        else if (Types.DOUBLE == m_sqlType)
        {
            Double inValue =
                (Double) ((value == null) ? Double.valueOf(DEFAULT_NUMERIC) : value);

            data.setDouble(inValue);
        }
        else if ((Types.FLOAT == m_sqlType) ||
                (Types.REAL == m_sqlType))
        {
            Double inValue = (Double) ((value == null) ? Double.valueOf(DEFAULT_NUMERIC) : value);

            data.setFloat(inValue);
        }
        else if (Types.INTEGER == m_sqlType)
        {
            Long inValue = (Long) ((value == null) ? Long.valueOf(DEFAULT_NUMERIC) : value);

            data.setInteger(inValue);
        }
        else if (Types.SMALLINT == m_sqlType)
        {
            Integer inValue = (Integer) ((value == null) ? Integer.valueOf(DEFAULT_NUMERIC) : value);

            data.setSmallInt(inValue);
        }
        else if (Types.TIME == m_sqlType)
        {
            Time inValue =
                (Time) ((value == null) ? new Time(defaultDate().getTime()) : value);

            data.setTime(inValue);
        }
        else if (Types.TIMESTAMP == m_sqlType)
        {
            Timestamp inValue =
                (Timestamp) ((value == null) ? new Timestamp(defaultDate().getTime()) : value);

            data.setTimestamp(inValue);
        }
        else if (Types.TINYINT == m_sqlType)
        {
            Short inValue = (Short) ((value == null) ? (short) DEFAULT_NUMERIC : value);

            data.setTinyInt(inValue);
        }
        else if ((Types.BINARY == m_sqlType) ||
                 (Types.VARBINARY == m_sqlType))
        {
            byte[] inValue = (byte[]) ((value == null) ? DEFAULT_STRING.getBytes() : value);

            data.setBinary(inValue);
        }
        else if (Types.LONGVARBINARY == m_sqlType)
       {
           byte[] inValue = (byte[]) ((value == null) ? DEFAULT_STRING.getBytes() : value);

           data.setLongVarBinary(inValue);
       }
        else if ((Types.CHAR == m_sqlType) ||
                 (Types.VARCHAR == m_sqlType) ||
                 (SqlType.TYPE_SQL_WCHAR == m_sqlType) ||
                 (SqlType.TYPE_SQL_WVARCHAR == m_sqlType))
        {
            String inValue = (String) ((value == null) ? DEFAULT_STRING : value);

            data.setVarChar(inValue);
        }
        else if ((Types.LONGVARCHAR == m_sqlType) || (SqlType.TYPE_SQL_WLONGVARCHAR == m_sqlType))
       {
           String inValue = (String) ((value == null) ? DEFAULT_STRING : value);

           data.setLongVarChar(inValue);
       }
        else if ((SqlType.TYPE_SQL_INTERVAL_DAY == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_DAY_TO_HOUR == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_DAY_TO_SECOND == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_HOUR == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_HOUR_TO_MINUTE == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_HOUR_TO_SECOND == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_MINUTE == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_MINUTE_TO_SECOND == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_SECOND == m_sqlType))
        {

            data.setInterval(new DSITimeSpan(m_sqlType, 4, 22, 45, 28, 0, false));
        }
        else if ((SqlType.TYPE_SQL_INTERVAL_MONTH == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_YEAR == m_sqlType) ||
                 (SqlType.TYPE_SQL_INTERVAL_YEAR_TO_MONTH == m_sqlType))
        {
            data.setInterval(new DSIMonthSpan(m_sqlType, 2011, 07, false));
        }
        else if (SqlType.TYPE_SQL_GUID == m_sqlType)
        {
            if (value instanceof String) 
            {
                data.setGuid(UUID.fromString((String) value));
            }
            else if (value instanceof UUID)
            {
                data.setGuid((UUID) value);
            }
        }
    }

}
