package com.datamigrate.exportimportservice.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VendorConfig {
    private String vendorId;
    private String displayName;
    private String operation;
    private String jdbcDriverClass;
    private String jdbcUrlTemplate;
    private int defaultPort;
    private List<ConnectionField> connectionDetails;
}
