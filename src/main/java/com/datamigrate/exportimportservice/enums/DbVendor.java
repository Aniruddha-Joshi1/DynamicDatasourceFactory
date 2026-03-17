package com.datamigrate.exportimportservice.enums;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum DbVendor {
    POSTGRES,
    MYSQL,
    SQLSERVER,
    ORACLE;

    @JsonCreator
    public static DbVendor fromString(String value){
        if (value == null || value.isBlank()){
            throw new IllegalArgumentException("Database vendore must not be blank");
        }
        try {
            return DbVendor.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException ex){
            throw new IllegalArgumentException("Unsupported database vendor: '" + value + "'. Supported values: postgres, sqlserver, mysql, oracle.");
        }
    }
}
