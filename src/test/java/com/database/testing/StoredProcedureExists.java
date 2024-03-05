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


public class StoredProcedureExists {
	Connection connection;
	Statement statement;
	ResultSet resultSet;
	CallableStatement cStatement;
	ResultSet resultSet1;
	ResultSet resultSet2;
	
	@BeforeMethod
	public void beforeMethod() throws SQLException {
		connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","root");
	}
	
	@Test(priority=1)
	public void test_storedProcedureExists() {
		try {
			
			statement = connection.createStatement();
			resultSet = statement.executeQuery("show procedure status where Name = 'SelectAllCustomers'");
			resultSet.next();
			Assert.assertEquals(resultSet.getString("Name"), "SelectAllCustomers");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/* { call procedure_name() }
	{ call procedure_name(?,?) }
	{?= call procedure_name() }
	{?= call procedure(?) } */
	@Test
	public void test_selectAllCustomers() {
		try {
			cStatement = connection.prepareCall("{call SelectAllCustomers()}");
			resultSet1 = cStatement.executeQuery();
			statement = connection.createStatement();
			resultSet2 = statement.executeQuery("select * from customers");
			Assert.assertEquals(compareResultSets(resultSet1,resultSet2), true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test_selectAllCustomersByCity() {
		try {
			cStatement = connection.prepareCall("{call SelectAllCustomersByCity('Singapore')}");
			resultSet1 = cStatement.executeQuery();
			
			statement = connection.createStatement();
			resultSet2 = statement.executeQuery("select * from customers where country='Singapore'");
			
			Assert.assertEquals(compareResultSets(resultSet1,resultSet2), true);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test_selectAllCustomersByCityAndPin() {
		try {
			cStatement = connection.prepareCall("{call SelectAllCustomersByCityAndPin('Las Vegas','83030')}");
			resultSet1 = cStatement.executeQuery();
			statement = connection.createStatement();
			resultSet2 = statement.executeQuery("select * from customers where city='Las Vegas' and postalCode='83030'");
			
			Assert.assertEquals(compareResultSets(resultSet1,resultSet2), true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test_get_order_info_by_cust() {
		try {
			cStatement = connection.prepareCall("{call get_order_info_by_cust(?,?,?,?,?,?,?)}");
			cStatement.setInt(1, 141);
			cStatement.registerOutParameter(2, Types.INTEGER);
			cStatement.registerOutParameter(3, Types.INTEGER);
			cStatement.registerOutParameter(4, Types.INTEGER);
			cStatement.registerOutParameter(5, Types.INTEGER);
			cStatement.registerOutParameter(6, Types.INTEGER);
			cStatement.registerOutParameter(7, Types.INTEGER);
			
			cStatement.executeQuery();
			
			int shipped = cStatement.getInt(2);
			int cancelled = cStatement.getInt(3);
			int resolved = cStatement.getInt(4);
			int disputed = cStatement.getInt(5);
			int in_process = cStatement.getInt(6);
			int on_hold = cStatement.getInt(7);
			
			System.out.println(shipped +" "+cancelled+" "+resolved+" "+disputed+" "+in_process+" "+on_hold);
			
			String query = "select (select count(*) from orders where customerNumber=141 and status='Shipped' ) as shipped,"
					+ "(select count(*) from orders where customerNumber=141 and status='Cancelled') as cancelled,"
					+ "(select count(*) from orders where customerNumber=141 and status='Resolved') as resolved,"
					+ "(select count(*) from orders where customerNumber=141 and status='Disputed') as disputed,"
					+ "(select count(*) from orders where customerNumber=141 and status='In Process') as in_process,"
					+ "(select count(*) from orders where customerNumber=141 and status='On Hold') as on_hold";
					
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			resultSet.next();
			int exp_shipped = resultSet.getInt("shipped");
			int exp_cancelled = resultSet.getInt("cancelled");
			int exp_resolved = resultSet.getInt("resolved");
			int exp_disputed = resultSet.getInt("disputed");
			int exp_in_process = resultSet.getInt("in_process");
			int exp_on_hold = resultSet.getInt("on_hold");
			
			System.out.println(exp_shipped +" "+exp_cancelled+" "+exp_resolved+" "+exp_disputed+" "+exp_in_process+" "+exp_on_hold);
			
			if(shipped==exp_shipped && cancelled==exp_cancelled && resolved==exp_resolved && disputed==exp_disputed && in_process==exp_in_process && on_hold==exp_on_hold) {
				Assert.assertTrue(true);
			}else {
				Assert.assertTrue(false);
			} 
			
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test_GetCustomerShippingDetails() {
		try {
			cStatement = connection.prepareCall("{call GetCustomerShippingDetails(?,?)}");
			cStatement.setInt(1, 141);
			cStatement.registerOutParameter(2, Types.VARCHAR);
			cStatement.executeQuery();
			String shippingTime = cStatement.getString(2);
			String query = "select country, "
					+ "case "
					+ 	"when country='USA' then '2-day shipping'"
					+ 	"when country='CANADA' then '3-day shipping'"
					+ 	"else '5-day'"
					+ "end as shippingTime from customers where customerNumber=141";
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			resultSet.next();
			String exp_shippingTime = resultSet.getString("shippingTime");
			
			Assert.assertEquals(shippingTime, exp_shippingTime);
			/* if(shippingTime.equals(exp_shippingTime)) {
				Assert.assertTrue(true);
			}else {
				Assert.assertTrue(false);
			} */
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) throws SQLException {
		while(resultSet1.next()) {
			resultSet2.next();
			int count = resultSet1.getMetaData().getColumnCount();
			for(int i=1;i<=count;i++) {
				if(!StringUtils.equals(resultSet1.getString(i),resultSet2.getString(i))) {
					return false;
				}
			}
		}
		return true;
	}
	
	@AfterMethod
	public void tearDown() throws SQLException {
		connection.close();
	}
}
