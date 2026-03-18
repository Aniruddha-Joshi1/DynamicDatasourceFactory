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
            throw new IllegalArgumentException("Database vendor must not be blank");
        }
        String normalizedValue = value
                .trim()
                .toUpperCase()
                .replace(" ", "");
        try {
            return DbVendor.valueOf(normalizedValue);
        } catch (IllegalArgumentException ex){
            throw new IllegalArgumentException("Unsupported database vendor: '" + value + "'. Supported values: postgres, sqlserver, mysql, oracle.");
        }
    }
}
