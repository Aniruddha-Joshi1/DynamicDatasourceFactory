package com.datamigrate.exportimportservice.controller;

import com.datamigrate.exportimportservice.model.*;
import com.datamigrate.exportimportservice.service.ExportService;
import com.datamigrate.exportimportservice.service.ImportService;
import com.datamigrate.exportimportservice.service.VendorConfigService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.exceptions.CsvValidationException;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@Slf4j
@RequestMapping("/api/datatransfer")
public class DataTransferController {
    private final ImportService importService;
    private final ExportService exportService;
    private final VendorConfigService vendorConfigService;

    @Autowired
    public DataTransferController(ImportService importService, ExportService exportService, VendorConfigService vendorConfigService){
        this.importService = importService;
        this.exportService = exportService;
        this.vendorConfigService = vendorConfigService;
    }

    @GetMapping("/vendor-config")
    public ResponseEntity<ApiResponse<VendorConfig>> getVendorConfig(@RequestParam String vendor) {
        return vendorConfigService.getVendorConfig(vendor)
                .map(config -> ResponseEntity.ok(ApiResponse.success("Vendor config fetched successfully", config)))
                .orElse(ResponseEntity.badRequest().body(ApiResponse.error("Unknown vendor: " + vendor)));
    }

    @PostMapping(value = "/publish", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<ApiResponse<ImportResponseModel>> importData(@RequestPart("file")MultipartFile file, @RequestPart("request") @Valid String requestJson) throws IOException, CsvValidationException {
        if (file.isEmpty()) {
            return ResponseEntity
                    .badRequest()
                    .body(ApiResponse.error("File cannot be empty"));
        }

        ObjectMapper mapper = new ObjectMapper();
        ImportExportRequestModel request = mapper.readValue(requestJson, ImportExportRequestModel.class);

        try {
            importService.importFile(file, request);
            return ResponseEntity.ok(ApiResponse.success("Data imported successfully", null));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(ApiResponse.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(ApiResponse.error("Import failed: " + e.getMessage()));
        }
    }

    @PostMapping("/export")
    public ResponseEntity<ApiResponse<ExportResponseModel>> exportData(@RequestBody ImportExportRequestModel request) {
        log.info("Export request received. Exporting from schema: {}, table: {}", request.getSchemaDetails().getSchema(), request.getSchemaDetails().getTableName());
        try{
            String outputFilePath = exportService.exportFile(request);
            return ResponseEntity.ok().body(ApiResponse.success("Data exported successfully to path: " + outputFilePath, null));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(ApiResponse.error(ex.getMessage()));
        } catch (Exception ex){
            log.error("Error while exporting: ", ex);
            return ResponseEntity.badRequest().body(ApiResponse.error("Export failed: " + ex.getMessage()));
        }
    }
}
