package com.datamigrate.exportimportservice.model;

import com.datamigrate.exportimportservice.enums.FileFormat;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ImportExportRequestModel {
    @NotNull(message = "Database configuration is required")
    private DatabaseConfigModel databaseConfig;

    @NotNull(message = "Schema details are required")
    private SchemaDetailsModel schemaDetails;

    private int batchSize = 100;

    private FileFormat fileFormat = FileFormat.CSV;
}
