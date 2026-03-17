package com.datamigrate.exportimportservice.model;

import com.datamigrate.exportimportservice.enums.DbVendor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DatabaseConfigModel {
    @NotBlank(message = "JDBC URL is required")
    private String url;

    @NotBlank(message = "Username is required")
    private String username;

    @NotBlank(message = "Password is required")
    private String password;

    @NotNull(message = "Database vendor is required")
    private DbVendor vendor;
}
