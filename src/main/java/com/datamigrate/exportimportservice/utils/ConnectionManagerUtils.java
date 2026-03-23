package com.datamigrate.exportimportservice.utils;

import com.datamigrate.exportimportservice.model.ConnectionField;
import com.datamigrate.exportimportservice.model.DatabaseConfigModel;
import com.datamigrate.exportimportservice.model.VendorConfig;
import com.datamigrate.exportimportservice.service.VendorConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ConnectionManagerUtils {
    private final VendorConfigService vendorConfigService;

    @Autowired
    public ConnectionManagerUtils(VendorConfigService vendorConfigService){
        this.vendorConfigService = vendorConfigService;
    }

    public Connection getConnection(DatabaseConfigModel dbDetails) {
        VendorConfig vendorConfig = vendorConfigService.getVendorConfig(dbDetails.getVendor())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unsupported vendor: '" + dbDetails.getVendor() + "'."));

        String jdbcUrl = buildJdbcUrl(vendorConfig, dbDetails.getConnectionFields());
        log.debug("Connecting to '{}' via URL: {}", dbDetails.getVendor(), jdbcUrl);

        try {
            Class.forName(vendorConfig.getJdbcDriverClass());
            return DriverManager.getConnection(jdbcUrl, dbDetails.getUsername(), dbDetails.getPassword());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC driver not found: " + vendorConfig.getJdbcDriverClass(), e);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to establish database connection: " + e.getMessage(), e);
        }
    }

    public static void closeConnection(Connection connection){
        if (connection != null){
            try{
                connection.close();
                log.info("Closed the jdbc connection");
            } catch (SQLException ex){
                log.warn("Failed to close connection: {}", ex.getMessage());
            }
        }
    }

    private String buildJdbcUrl(VendorConfig vendorConfig, Map<String, String> connectionFields) {
        // Validate required  (could be done by the UI but still)
        vendorConfig.getConnectionDetails().stream()
                .filter(ConnectionField::isRequired)
                .forEach(field -> {
                    String value = connectionFields.get(field.getName());
                    if (value == null || value.isBlank()) {
                        throw new IllegalArgumentException(
                                "Missing required connection field: '" + field.getName() + "'");
                    }
                });

        final String template = vendorConfig.getJdbcUrlTemplate();

        // Fields that have an actual {token} in the template get substituted directly into the URL.
        // Everything else gets appended as a query param.
        Set<String> substitutedFields = vendorConfig.getConnectionDetails().stream()
                .map(ConnectionField::getName)
                .filter(name -> template.contains("{" + name + "}"))
                .collect(Collectors.toSet());

        String url = template;

        // Substitute tokens with their values in the url
        for (var field : vendorConfig.getConnectionDetails()) {
            String value = connectionFields.get(field.getName());
            if (value == null && field.getDefaultValue() != null) {
                value = field.getDefaultValue().toString();
            }
            if (value != null) {
                url = url.replace("{" + field.getName() + "}", value);
            }
        }

        // Any token in the template, which was not a part of the url already,
        // gets appended as a query param — e.g. ssl=true, connectTimeout=30
        String extraParams = connectionFields.entrySet().stream()
                .filter(e -> !substitutedFields.contains(e.getKey()))
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("&"));

        if (!extraParams.isBlank()) {
            url = url.contains("?") ? url + "&" + extraParams : url + "?" + extraParams;
        }

        // Clean up dangling separators
        url = url.replaceAll(";+$", "").replaceAll(";{2,}", ";");
        url = url.replaceAll("\\?$", "").replaceAll("\\?&", "?").replaceAll("&{2,}", "&");

        return url;
    }
}
