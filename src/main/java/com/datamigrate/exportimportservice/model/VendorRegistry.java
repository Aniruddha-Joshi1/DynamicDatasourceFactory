package com.datamigrate.exportimportservice.model;

import lombok.Data;

import java.util.List;

@Data
public class VendorRegistry {
    private List<VendorSummary> vendors;
}
