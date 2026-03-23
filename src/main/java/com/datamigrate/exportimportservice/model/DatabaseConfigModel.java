package com.datamigrate.exportimportservice.model;

import com.datamigrate.exportimportservice.enums.DbVendor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.Map;

@Data
public class DatabaseConfigModel {
    @NotBlank(message = "Database vendor is required")
    private String vendor;

    @NotNull(message = "Connection fields are required")
    private Map<String, String> connectionFields;

    @NotBlank(message = "Username is required")
    private String username;

    @NotBlank(message = "Password is required")
    private String password;
}
