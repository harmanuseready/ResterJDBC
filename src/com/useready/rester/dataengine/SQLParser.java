package com.useready.rester.dataengine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.simba.support.ILogger;
import com.simba.support.LogUtilities;
import com.useready.rester.Settings;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

public class SQLParser {
	private static final Set<String> AGGREGATE_FUNCTIONS = new HashSet<>(
			Arrays.asList("SUM", "AVG", "MIN", "MAX", "COUNT"));

	public static Map<String, String> extractColumnNameSQLParser(String sqlQuery, Settings m_settings,
			ILogger m_logger) {
		Map<String, String> columnNames = new HashMap<>();
		LogUtilities.logDebug("extractColumnNameSQLParser::", m_logger);
		try {
			Statement statement = CCJSqlParserUtil.parse(sqlQuery);

			if (statement instanceof Select) {
				Select selectStatement = (Select) statement;
				SelectBody selectBody = selectStatement.getSelectBody();
				LogUtilities.logDebug("extractColumnNameSQLParser  statement::", m_logger);
				if (selectBody instanceof PlainSelect) {
					PlainSelect plainSelect = (PlainSelect) selectBody;

					// Initialize an expression visitor
					ExpressionVisitorAdapter expressionVisitor = new ExpressionVisitorAdapter() {
						@Override
						public void visit(Function function) {
							String functionName = function.getName().toUpperCase();
							if (AGGREGATE_FUNCTIONS.contains(functionName)) {
								String[] parts = (function.getParameters().toString()).replace("\"", "").split("\\.");

								// The last part of the split result should be the actual column name
								String cleanedColumnName = parts[parts.length - 1];
								cleanedColumnName=wrapWithSingleQuotes(cleanedColumnName);
								String aggregationFunction = functionName + "(" + cleanedColumnName + ")";
								// String aggregationFunction = functionName +"("+function.getParameters()+")";
								String columnName = removeTableAlias(aggregationFunction, m_settings.m_TableName);
								columnNames.put(columnName, columnName);
								String str=removeTableAlias((function.getParameters().toString()), m_settings.m_TableName);
								LogUtilities.logDebug("extractColumnNameSQLParser::", m_logger);
								m_settings.m_columnData.put(columnName,
										str.replace("\"", ""));
							} else if (functionName.contains("TRUNC")) {
								Expression parameter = function.getParameters().getExpressions().get(0);
								if (parameter instanceof Column) {
									Column column = (Column) parameter;
									String columnName = column.getColumnName();
									columnName = columnName.replace("\"", "");
									columnNames.put(columnName, columnName);
									m_settings.m_columnData.put(columnName, columnName);
								} else if (parameter instanceof Function) {
									Function function1 = (Function) parameter;
									String columnName = removeTableAlias(function1.getParameters().toString(),
											m_settings.m_TableName);
									columnName = columnName.replace("\"", "");
									columnNames.put(columnName, columnName);
									m_settings.m_columnData.put(columnName, columnName);
								}
							}
						}

						@Override
						public void visit(Column column) {
							String columnName = column.getColumnName();
							columnName = columnName.replace("\"", "");
							columnNames.put(columnName, columnName);
							m_settings.m_columnData.put(columnName, columnName);
						}
					};

					// Traverse the expressions in the SELECT statement
					for (SelectItem item : plainSelect.getSelectItems()) {
						if (item instanceof SelectExpressionItem) {
							SelectExpressionItem expressionItem = (SelectExpressionItem) item;
							Expression expression = expressionItem.getExpression();
							expression.accept(expressionVisitor);
						}
					}
				} else {
					System.out.println("This is not a valid PlainSelect statement.");
				}
			} else {
				System.out.println("This is not a valid SELECT statement.");
			}
		} catch (JSQLParserException e) {
			LogUtilities.logDebug("Exception  ::::::::::::::;;;" + e.getMessage(), m_logger);
			e.printStackTrace();
		}
		return columnNames;

	}

	private static String removeTableAlias(String input, String alias) {
		return input.replaceAll("\"" + alias + "\"\\.", "");
	}

	  public static String wrapWithSingleQuotes(String input) {
		  Pattern pattern = Pattern.compile("[!@#$%^&*()\\s]+");

	        // Use a Matcher to find matches in the input string
	        Matcher matcher = pattern.matcher(input);

	        // Return true if a match is found, indicating the presence of special characters
	        if( matcher.find()) {
	        return "'" + input + "'";
	        }
			return input;
	    }
	private static String bindStringWithSingleQuotes(String input) {
		StringBuilder result = new StringBuilder();

		for (int i = 0; i < input.length(); i++) {
			char c = input.charAt(i);
			if (!Character.isLetterOrDigit(c)) {
				result.append('\'');
			}
			result.append(c);
			if (!Character.isLetterOrDigit(c)) {
				result.append('\'');
			}
		}

		return result.toString();
	}

}
