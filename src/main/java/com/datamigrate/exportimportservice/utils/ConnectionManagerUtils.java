package com.datamigrate.exportimportservice.utils;

import com.datamigrate.exportimportservice.constants.DriverConstants;
import com.datamigrate.exportimportservice.exception.ConnectionException;
import com.datamigrate.exportimportservice.model.DatabaseConfigModel;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class ConnectionManagerUtils {
    public static Connection getConnection(DatabaseConfigModel dbDetails) {
        String driverClassName = DriverConstants.getDriverClassName(dbDetails.getVendor());
        try{
            Class.forName(driverClassName);
        } catch (ClassNotFoundException ex) {
            log.error("Driver not found");
            throw new ConnectionException("JDBC driver not found: " + driverClassName, ex);
        }

        try{
            Connection connection = DriverManager.getConnection(dbDetails.getUrl(), dbDetails.getUsername(), dbDetails.getPassword());
            log.info("Connection established to: {}", dbDetails.getUrl());
            return connection;
        } catch (SQLException ex){
            log.error("Failed to connect to the database: {}", dbDetails.getUrl());
            throw new ConnectionException("Failed to connect to: " + dbDetails.getUrl(), ex);
        }
    }

    public static void closeConnection(Connection connection){
        if (connection != null){
            try{
                connection.close();
            } catch (SQLException ex){
                log.warn("Failed to close connection: {}", ex.getMessage());
            }
        }
    }
}
