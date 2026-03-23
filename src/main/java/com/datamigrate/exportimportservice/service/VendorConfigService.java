package com.datamigrate.exportimportservice.service;

import com.datamigrate.exportimportservice.model.ConnectionField;
import com.datamigrate.exportimportservice.model.VendorConfig;
import com.datamigrate.exportimportservice.model.VendorRegistry;
import com.datamigrate.exportimportservice.model.VendorSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class VendorConfigService {
    private static final String REGISTRY_PATH = "vendors/vendors-registry.json";
    private static final String VENDOR_DIR = "vendors/";

    private final ObjectMapper objectMapper;
    private Map<String, VendorSummary> summaryIndex;

    @PostConstruct
    public void init() throws IOException {
        ClassPathResource resource = new ClassPathResource(REGISTRY_PATH);
        try (InputStream is = resource.getInputStream()) {
            VendorRegistry registry = objectMapper.readValue(is, VendorRegistry.class);

            summaryIndex = new LinkedHashMap<>();
            for (VendorSummary summary : registry.getVendors()) {
                summaryIndex.put(summary.getVendorId().toLowerCase(), summary);
            }
            log.info("Loaded vendor registry index with {} vendors: {}",
                    summaryIndex.size(), summaryIndex.keySet());
            log.info("Full vendor configs will be loaded lazily on first access.");
        }
    }

    public List<VendorSummary> getAllVendorSummaries() {
        return List.copyOf(summaryIndex.values());
    }

    public Optional<VendorConfig> getVendorConfig(String vendorId){
        String key = vendorId.toLowerCase();
        if (!summaryIndex.containsKey(key)) {
            return Optional.empty();
        }
        VendorConfig vendorConfig = loadVendorConfig(key);
        return Optional.ofNullable(vendorConfig);
    }

    public Optional<List<ConnectionField>> getConnectionFields(String vendorId) {
        return getVendorConfig(vendorId).map(VendorConfig::getConnectionDetails);
    }

    private VendorConfig loadVendorConfig(String vendorId) {
        String resourcePath = VENDOR_DIR + vendorId + ".json";
        ClassPathResource resource = new ClassPathResource(resourcePath);

        if (!resource.exists()) {
            log.error("Vendor config file not found on classpath: '{}'. " +
                    "Add the file at src/main/resources/{}", resourcePath, resourcePath);
            return null;
        }

        try (InputStream is = resource.getInputStream()) {
            VendorConfig config = objectMapper.readValue(is, VendorConfig.class);
            log.info("Lazily loaded vendor config for '{}' from '{}'", vendorId, resourcePath);
            return config;
        } catch (IOException e) {
            log.error("Failed to parse vendor config '{}': {}", resourcePath, e.getMessage(), e);
            return null;
        }
    }
}
