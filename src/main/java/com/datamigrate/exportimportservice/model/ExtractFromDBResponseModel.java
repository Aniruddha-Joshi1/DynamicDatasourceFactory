package com.datamigrate.exportimportservice.model;

import lombok.Data;

@Data
public class ExtractFromDBResponseModel {
    private String message;
    private String pathToCsv;
}
