package com.database.testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

public class GetDataFromDatabase {
	
	static Connection connection;
	static Statement statement;
	static ResultSet resultSet;
	
	
	public static void main(String[] args) {
		String query = "select * from customers where customerNumber=103";
		try {
			connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","root");
			if(connection.isClosed()) {
				System.out.println("We not connected to database");
			}else {
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			while(resultSet.next()) {
				System.out.println(resultSet.getInt("customerNumber"));
				System.out.println(resultSet.getString("customerName"));
				System.out.println(resultSet.getString("city"));
				System.out.println(resultSet.getString("country"));
				System.out.println("temporary text by mbm");
			}
		}
		}catch (SQLException e) {
			e.printStackTrace();
		}
	}
		
}
