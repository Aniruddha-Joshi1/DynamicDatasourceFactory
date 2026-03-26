package com.datamigrate.exportimportservice.enums;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum FileFormat {
    CSV, PARQUET;

    @JsonCreator
    public static FileFormat from(String value){
        if (value == null) return CSV;
        return switch (value.toUpperCase()) {
            case "PARQUET" -> PARQUET;
            default -> CSV;
        };
    }
}
