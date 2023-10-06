import static org.junit.Assert.assertEquals;
import org.junit.Test;
import com.useready.rester.Settings;
import com.useready.rester.dataengine.SQLParser;

import java.util.Map;

public class SQLParserTest {

    @Test
    public void testExtractColumnName() {
        // Define your test SQL query
        String sqlQuery = "SELECT \"Fields\".\"FxRates.Currancy\" AS \"FxRates.Currancy\", " +
                "SUM(\"Fields\".\"Measures.ACLS\") AS \"sum:Measures.ACLS:ok\" " +
                "FROM \"CPRT_EqVolSlidePGArms 372326777 \".\"Fields\" \"Fields\" " +
                "GROUP BY 1";

        // Create a Settings object with the table name
        Settings settings = new Settings();
        settings.m_TableName = "Fields";

        // Call the extractColumnName method to get the result
       // Map<String, String> columnNames = SQLParser.extractColumnNameSQLParser(sqlQuery, settings);

        // Assert the expected results
        //assertEquals("FxRates.Currancy", columnNames.get("FxRates.Currancy"));
        //assertEquals("SUM(Measures.ACLS)", columnNames.get("SUM(Measures.ACLS)"));
    }
}
