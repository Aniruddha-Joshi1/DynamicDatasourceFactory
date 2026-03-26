package com.datamigrate.exportimportservice.service;

import com.datamigrate.exportimportservice.model.ImportExportRequestModel;
import com.datamigrate.exportimportservice.utils.ConnectionManagerUtils;
import com.datamigrate.exportimportservice.utils.ParquetInputFileUtils;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

    public void importFile(MultipartFile file, ImportExportRequestModel request) throws CsvValidationException, IOException {
        String fileName = file.getOriginalFilename();
        if (fileName == null){
            throw new IllegalArgumentException("File name is missing");
        }
        fileName = fileName.toLowerCase();
        if(fileName.endsWith(".csv")){
            log.info("Import request received for CSV file = '{}' into table ='{}'", fileName, request.getSchemaDetails().getTableName());
            importFileFromCSV(file, request);
        } else if (fileName.endsWith(".parquet")) {
            log.info("Import request received for Parquet file = '{}' into table ='{}'", fileName, request.getSchemaDetails().getTableName());
            importFileFromParquet(file, request);
        } else {
            throw new IllegalArgumentException("Unsupported file type. Currently only CSV and PARQUET file are supported");
        }
    }

    public void importFileFromCSV(MultipartFile file, ImportExportRequestModel importRequest) throws CsvValidationException, IOException {
        String fileName = file.getOriginalFilename();
        log.info("Starting import for the file='{}' and it will be imported into table = '{}'", fileName,
                importRequest.getSchemaDetails().getTableName());

        Connection connection = connectionManagerUtils.getConnection(importRequest.getDatabaseConfig());
        try {
            JdbcTemplate jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            readFileAndInsertDataFromCSV(file, jdbc, importRequest);
        } finally {
            connectionManagerUtils.closeConnection(connection);
        }
    }

    public void importFileFromParquet(MultipartFile file, ImportExportRequestModel importRequest) throws IOException{
        log.info("Starting Parquet import for the file = '{}' into table = '{}'", file.getOriginalFilename(), importRequest.getSchemaDetails().getTableName());

        Path tempFile = Files.createTempFile("parquet-import-", ".parquet");
        try{
            Files.write(tempFile, file.getBytes());
            Connection connection = connectionManagerUtils.getConnection(importRequest.getDatabaseConfig());
            try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInputFileUtils(tempFile))) {
                // gets the schema
                MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                String[] columns = schema.getFields().stream().map(Type::getName).toArray(String[]::new);
                String insertSql = buildInsertSql(extractedTableName(importRequest), columns);
                int batchSize = importRequest.getBatchSize();
                JdbcTemplate jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
                List<Object[]> batch = new ArrayList<>(batchSize);
                // MessageColumnIO is used to change the columns into rows
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                // PageReadStore stores data for one row group, in 1 row group there could be multiple columns of data
                PageReadStore pages;
                while((pages = reader.readNextRowGroup()) != null){
                    long rowCount = pages.getRowCount();
                    // builds rows one by one from the column data in parquet files
                    RecordReader<Group> recordReader = columnIO.getRecordReader(
                            pages, new GroupRecordConverter(schema));

                    for (long i = 0; i < rowCount; i++) {
                        Group group = recordReader.read();
                        Object[] row = extractRowFromGroup(group, schema);
                        batch.add(row);

                        if (batch.size() == batchSize) {
                            insertBatchIntoDB(jdbc, insertSql, batch);
                            batch.clear();
                        }
                    }
                }
                if (!batch.isEmpty()){
                    insertBatchIntoDB(jdbc, insertSql, batch);
                }
            } finally {
                connectionManagerUtils.closeConnection(connection);
            }
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private Object[] extractRowFromGroup(Group group, MessageType schema) {
        int fieldCount = schema.getFieldCount();
        Object[] row   = new Object[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            Type field = schema.getType(i);

            // Null check — field may be optional and absent in this row
            if (group.getFieldRepetitionCount(i) == 0) {
                row[i] = null;
                continue;
            }

            // Map Parquet primitive type → Java type
            row[i] = switch (field.asPrimitiveType().getPrimitiveTypeName()) {
                case INT32   -> group.getInteger(i, 0);
                case INT64   -> group.getLong(i, 0);
                case FLOAT   -> group.getFloat(i, 0);
                case DOUBLE  -> group.getDouble(i, 0);
                case BOOLEAN -> group.getBoolean(i, 0);
                case BINARY,
                     FIXED_LEN_BYTE_ARRAY -> group.getString(i, 0);
                default      -> group.getValueToString(i, 0);
            };
        }
        return row;
    }

    private void readFileAndInsertDataFromCSV(MultipartFile file, JdbcTemplate jdbc, ImportExportRequestModel importRequest) throws IOException, CsvValidationException {
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

    private void insertBatchIntoDB(JdbcTemplate jdbc, String sqlQuery, List<Object[]> batchData){
        try{
            jdbc.batchUpdate(sqlQuery, batchData);
        } catch (Exception e){
            log.error("Batch insert failed (batch size = {}): {}", batchData.size(), e.getMessage());
        }
    }

    private String buildInsertSql(String tableName, String[] headers){
        String columns = String.join(", ", headers);
        String placeholders = "?, ".repeat(headers.length);
        placeholders = placeholders.substring(0, placeholders.length()-2);
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    private String extractedTableName(ImportExportRequestModel importRequest){
        String schema = importRequest.getSchemaDetails().getSchema();
        String table = importRequest.getSchemaDetails().getTableName();
        if (schema != null && !schema.isBlank()){
            return schema + "." + table;
        }
        return table;
    }
}
