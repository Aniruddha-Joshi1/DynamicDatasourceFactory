package com.datamigrate.exportimportservice.service;

import com.datamigrate.exportimportservice.model.ImportExportRequestModel;
import com.datamigrate.exportimportservice.utils.ConnectionManagerUtils;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Service
public class ImportService {
    // Step1: Read the file and store the data in a data structure
    // Step2: Write an insert query with the data
    // Step3: Execute the insert query using the jdbc connection to insert data into DB
    private final ConnectionManagerUtils connectionManagerUtils;

    @Autowired
    public ImportService(ConnectionManagerUtils connectionManagerUtils){
        this.connectionManagerUtils = connectionManagerUtils;
    }

    public void importFromFile(MultipartFile file, ImportExportRequestModel importRequest) throws CsvValidationException, IOException {
        String fileName = file.getOriginalFilename();
        log.info("Starting import for the file='{}' and it will be imported into table = '{}'", fileName,
                importRequest.getSchemaDetails().getTableName());

        Connection connection = connectionManagerUtils.getConnection(importRequest.getDatabaseConfig());
        try {
            JdbcTemplate jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            readFileAndInsertData(file, jdbc, importRequest);
        } finally {
            connectionManagerUtils.closeConnection(connection);
        }
    }

    public void readFileAndInsertData(MultipartFile file, JdbcTemplate jdbc, ImportExportRequestModel importRequest) throws IOException, CsvValidationException {
        try (CSVReader csvReader = new CSVReader(
                new BufferedReader(
                        new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8)
                )
            )
        ) {
            String[] headers = csvReader.readNext();
            if (headers == null || headers.length == 0){
                throw new IllegalArgumentException("CSV file has no header row");
            }
            headers = Arrays.stream(headers).map(String::trim).toArray(String[]::new);

            String insertSqlQuery = buildInsertSql(extractedTableName(importRequest), headers);
            int batchSize = importRequest.getBatchSize();
            List<Object[]> batch = new ArrayList<>(batchSize);
            String[] row;
            while((row = csvReader.readNext()) != null){
                batch.add(row);
                if(batch.size()==batchSize){
                    insertBatchIntoDB(jdbc, insertSqlQuery, batch);
                    batch.clear();
                }
            }

            if(!batch.isEmpty()){
                insertBatchIntoDB(jdbc, insertSqlQuery, batch);
            }
        }
    }

    public void insertBatchIntoDB(JdbcTemplate jdbc, String sqlQuery, List<Object[]> batchData){
        try{
            jdbc.batchUpdate(sqlQuery, batchData);
        } catch (Exception e){
            log.error("Batch insert failed (batch size = {}): {}", batchData.size(), e.getMessage());
        }
    }

    public String buildInsertSql(String tableName, String[] headers){
        String columns = String.join(", ", headers);
        String placeholders = "?, ".repeat(headers.length);
        placeholders = placeholders.substring(0, placeholders.length()-2);
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    public String extractedTableName(ImportExportRequestModel importRequest){
        String schema = importRequest.getSchemaDetails().getSchema();
        String table = importRequest.getSchemaDetails().getTableName();
        if (schema != null && !schema.isBlank()){
            return schema + "." + table;
        }
        return table;
    }
}
