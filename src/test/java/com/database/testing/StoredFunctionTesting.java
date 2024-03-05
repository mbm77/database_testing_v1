package com.database.testing;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StoredFunctionTesting {

	Connection connection;
	Statement statement;
	ResultSet resultSet;
	ResultSet resultSet2;
	CallableStatement cStatement;

	@BeforeMethod
	public void beforeMethod() {
		try {
			connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels", "root", "root");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void storedFunctionExists_Test() {
		try {
			statement = connection.createStatement();
			resultSet = statement.executeQuery("show function status where db='classicmodels'");
			resultSet.next();
			Assert.assertEquals(resultSet.getString("Name"), "customerLevel");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void verifyStoredFunctionWithSqlQuery() throws SQLException {
		resultSet = connection.createStatement()
				.executeQuery("select customerName, customerLevel(creditLimit) from customers");
		resultSet2 = connection.createStatement()
				.executeQuery("select customerName," + "case " + "when creditLimit > 50000 then 'Platinum'"
						+ "when creditLimit < 50000 and creditLimit >= 10000 then 'Gold'"
						+ "when creditLimit < 10000 then 'Silver' end as customerLevel from customers;");

		Assert.assertEquals(compareResultSets(resultSet, resultSet2), true);

	}

	@Test
	public void verifyStoredProcedureWithStoredFunctionAndSqlQuery() throws SQLException {
		cStatement = connection.prepareCall("{call getCustomerLevel(?,?)}");
		cStatement.setInt(1, 141);
		cStatement.registerOutParameter(2, Types.VARCHAR);
		cStatement.executeQuery();
		String customerLevelStr = cStatement.getString(2);
		
		
		statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(""
				+ "select customerName, "
				+ "case "
				+ "	when creditLimit > 50000 then 'Platinum'"
				+ "	when creditLimit < 50000 and creditLimit >10000 then 'Gold'"
				+ "	when creditLimit < 10000 then 'Silver'"
				+ "end as customerLevel from customers where customerNumber='"+141+"';");
		resultSet.next();
		String exp_custLevel = resultSet.getString("customerLevel");
		Assert.assertEquals(customerLevelStr, exp_custLevel);
	}

	public boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) throws SQLException {
		while (resultSet1.next()) {
			resultSet2.next();
			int count = resultSet1.getMetaData().getColumnCount();
			for (int i = 1; i <= count; i++) {
				if (!StringUtils.equals(resultSet1.getString(i), resultSet2.getString(i))) {
					return false;
				}
			}
		}
		return true;
	}

	@AfterMethod
	public void afterMethod() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
