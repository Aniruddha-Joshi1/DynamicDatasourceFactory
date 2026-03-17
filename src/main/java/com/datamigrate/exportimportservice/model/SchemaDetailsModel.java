package com.datamigrate.exportimportservice.model;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class SchemaDetailsModel {
    // If not given, then the default schema of the user will be used
    private String schema;

    @NotBlank(message = "Target table name is required")
    private String tableName;
}
