import com.useready.rester.dataengine.ExtractColumnName;
import com.useready.rester.dataengine.URDataEngine;



import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

public class TestSQLQuery {

    @Test
    public void testExtractColumnName() {
    	//ExtractColumnName yourClass = new ExtractColumnName(); // Replace with the actual class containing your method
        String sqlQuery ="SELECT CAST(TRUNCATE(DAY(\"Fields\".\"null.SubmitDate\"),0) AS INTEGER) AS \"dy:null.SubmitDate:ok\", "
                + "CAST(TRUNCATE(MONTH(\"Fields\".\"null.SubmitDate\"),0) AS INTEGER) AS \"mn:null.SubmitDate:ok\", "
                + "CAST(TRUNCATE(QUARTER(\"Fields\".\"null.SubmitDate\"),0) AS INTEGER) AS \"qr:null.SubmitDate:ok\", "
                + "CAST(TRUNCATE(YEAR(\"Fields\".\"null.SubmitDate\"),0) AS INTEGER) AS \"yr:null.SubmitDate:ok\" "
                + "FROM \"EqPriceReftData 372328387 \".\"Fields\" \"Fields\" "
                + "GROUP BY 1, 2, 3, 4";
        Map<String, String> result = ExtractColumnName.extractColumnName(sqlQuery);

        assertEquals(1, result.size());
     //   assertEquals("null.SubmitDate", result.get("null.SubmitDate"));
        assertEquals("null.SubmitDate", result.get("null.SubmitDate"));
//        assertEquals("column2", result.get("alias2"));
    }
    @Test
    public void testStringDataype() {
    	//ExtractColumnName yourClass = new ExtractColumnName(); // Replace with the actual class containing your method
    	String sqlQuery = "SELECT \"Fields\".\"FxRates.Currancy\" AS \"FxRates.Currancy\" "
                + "FROM \"EqPriceReftData 372328387 \".\"Fields\" \"Fields\" "
                + "GROUP BY 1";
        Map<String, String> result = ExtractColumnName.extractColumnName(sqlQuery);

        assertEquals(1, result.size());
     //   assertEquals("null.SubmitDate", result.get("null.SubmitDate"));
        assertEquals("FxRates.Currancy", result.get("FxRates.Currancy"));
//        assertEquals("column2", result.get("alias2"));
    }
    @Test
    public void testIntegerDataype() {
    	//ExtractColumnName yourClass = new ExtractColumnName(); // Replace with the actual class containing your method
        String sqlQuery = "SELECT \"Fields\".\"FxRates.Rate\" AS \"FxRates.Rate\" "
                + "FROM \"EqPriceReftData 372328387 \".\"Fields\" \"Fields\" "
                + "GROUP BY 1";;
        Map<String, String> result = ExtractColumnName.extractColumnName(sqlQuery);

        assertEquals(1, result.size());
     //   assertEquals("null.SubmitDate", result.get("null.SubmitDate"));
        assertEquals("FxRates.Rate", result.get("FxRates.Rate"));
//        assertEquals("column2", result.get("alias2"));
    }
    @Test
    public void testBooleanDataType() {
        String sqlQuery = "SELECT \"Fields\".\"FxRates.IsRiskFree\" AS \"FxRates.IsRiskFree\" "
                + "FROM \"EqPriceReftData 372328387 \".\"Fields\" \"Fields\" "
                + "GROUP BY 1";
        String fieldName=null;

        Map<String, String> result = ExtractColumnName.extractColumnName(sqlQuery);

        assertEquals(1, result.size());
     //   assertEquals("null.SubmitDate", result.get("null.SubmitDate"));
        assertEquals("FxRates.IsRiskFree", result.get("FxRates.IsRiskFree"));
    }
    
    @Test
    public void testMeasureQuery() {
        String sqlQuery = "SELECT SUM(\"Fields\".\"Measures.ACLS\") AS \"sum:Measures.ACLS:ok\" "
                + "FROM \"EqPriceReftData 372328387 \".\"Fields\" \"Fields\" "
                + "HAVING (COUNT(1) > 0)";
        String fieldName=null;

        fieldName = ExtractColumnName.extractAggregationData(sqlQuery, fieldName);
        System.out.println(":::::::::::::::::::::"+fieldName);
        assertEquals("\"SUM(ACLS)\"", fieldName);
    }
    
    
    
}

