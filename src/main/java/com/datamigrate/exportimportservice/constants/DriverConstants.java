package com.datamigrate.exportimportservice.constants;

import com.datamigrate.exportimportservice.enums.DbVendor;

import java.util.Map;

public class DriverConstants {
    private static final Map<DbVendor, String> DRIVER_MAP = Map.of(
            DbVendor.POSTGRES, "org.postgresql.Driver",
            DbVendor.MYSQL, "com.mysql.cj.jdbc.Driver",
            DbVendor.SQLSERVER, "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            DbVendor.ORACLE, "oracle.jdbc.OracleDriver"
    );

    public static String getDriverClassName(DbVendor dbVendor){
        String driverClass = DRIVER_MAP.get(dbVendor);
        if (driverClass == null){
            throw new IllegalArgumentException("No driver class registered for vendor: " + dbVendor);
        }
        return driverClass;
    }
}
