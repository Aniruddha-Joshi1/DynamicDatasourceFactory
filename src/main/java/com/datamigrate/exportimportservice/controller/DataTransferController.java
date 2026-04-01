package com.datamigrate.exportimportservice.controller;

import com.datamigrate.exportimportservice.model.*;
import com.datamigrate.exportimportservice.service.ExtractFromDBService;
import com.datamigrate.exportimportservice.service.PublishIntoDBService;
import com.datamigrate.exportimportservice.service.VendorConfigService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.exceptions.CsvValidationException;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@Slf4j
@RequestMapping("/api/datatransfer")
public class DataTransferController {
    private final PublishIntoDBService publishIntoDBService;
    private final ExtractFromDBService extractFromDBService;
    private final VendorConfigService vendorConfigService;

    @Autowired
    public DataTransferController(PublishIntoDBService publishIntoDBService, ExtractFromDBService extractFromDBService, VendorConfigService vendorConfigService){
        this.publishIntoDBService = publishIntoDBService;
        this.extractFromDBService = extractFromDBService;
        this.vendorConfigService = vendorConfigService;
    }

    @GetMapping("/vendor-config")
    public ResponseEntity<ApiResponse<VendorConfig>> getVendorConfig(@RequestParam String vendor) {
        return vendorConfigService.getVendorConfig(vendor)
                .map(config -> ResponseEntity.ok(ApiResponse.success("Vendor config fetched successfully", config)))
                .orElse(ResponseEntity.badRequest().body(ApiResponse.error("Unknown vendor: " + vendor)));
    }

    @PostMapping(value = "/publishToDB")
    public ResponseEntity<ApiResponse<PublishToDBResponseModel>> publishDataIntoTheFile(@RequestBody @Valid String requestJson) throws IOException, CsvValidationException {

        ObjectMapper mapper = new ObjectMapper();
        PublishAndExtractFromDBRequestModel request = mapper.readValue(requestJson, PublishAndExtractFromDBRequestModel.class);

        try {
            publishIntoDBService.publishDataIntoDB(request);
            return ResponseEntity.ok(ApiResponse.success("Data imported successfully", null));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(ApiResponse.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(ApiResponse.error("Import failed: " + e.getMessage()));
        }
    }

    @PostMapping("/extractFromDB")
    public ResponseEntity<ApiResponse<ExtractFromDBResponseModel>> extractDataFromTheFile(@RequestBody PublishAndExtractFromDBRequestModel request) {
        log.info("Exportacting from schema: {}, table: {}", request.getSchemaDetails().getSchema(), request.getSchemaDetails().getTableName());
        try{
            String outputFilePath = extractFromDBService.extractDataFromFile(request);
            return ResponseEntity.ok().body(ApiResponse.success("Data extracted successfully and saved to the path: " + outputFilePath, null));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(ApiResponse.error(ex.getMessage()));
        } catch (Exception ex){
            log.error("Error while extracting: ", ex);
            return ResponseEntity.badRequest().body(ApiResponse.error("Extracting failed: " + ex.getMessage()));
        }
    }
}
