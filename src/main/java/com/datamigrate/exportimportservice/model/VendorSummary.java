package com.datamigrate.exportimportservice.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL) // suppresses Jackson from serializing null values
public class VendorSummary {
    private String vendorId;
    private String displayName;
    private String operation;
    private int defaultPort;
}
