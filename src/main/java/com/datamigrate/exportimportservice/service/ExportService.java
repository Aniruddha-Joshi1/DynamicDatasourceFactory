package com.datamigrate.exportimportservice.service;

import com.datamigrate.exportimportservice.model.ImportExportRequestModel;
import com.datamigrate.exportimportservice.utils.ConnectionManagerUtils;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

@Service
@Slf4j
public class ExportService {
    // Step1: Store the data from the DB
    // Step2: Create a directory to store the exported files (Store with unique file names)
    // Step3: Create a file and put the data extracted from the DB into the file. Headers will be the column names which are comma separated.
    @Value("${app.output.dir}")
    Path outputDir;

    public String exportToCsv(ImportExportRequestModel request){
        String sqlQuery = buildSqlQuery(request);
        log.info("Starting export from the schema='{}' and table = '{}'", request.getSchemaDetails().getSchema(),
                request.getSchemaDetails().getTableName());
        String fileName = generateFileName(request.getSchemaDetails().getTableName());
        Path outdirPath = Paths.get(outputDir.toUri());
        Path filePath = outdirPath.resolve(fileName);
        Connection connection = ConnectionManagerUtils.getConnection(request.getDatabaseConfig());
        try{
            if(!Files.exists(outdirPath)){
                Files.createDirectories(outputDir);
            }
            JdbcTemplate jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            return retrieveAndExportToCsv(sqlQuery, jdbcTemplate, filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            ConnectionManagerUtils.closeConnection(connection);
        }
    }

    private String buildSqlQuery(ImportExportRequestModel request) {
        StringBuilder query = new StringBuilder("SELECT * FROM ");
        query.append(request.getSchemaDetails().getSchema());
        query.append(".");
        query.append(request.getSchemaDetails().getTableName());
        return query.toString();
    }

    private String generateFileName(String tableName){
        return tableName + "_" + System.currentTimeMillis() + ".csv";
    }

    private String retrieveAndExportToCsv(String sqlQuery, JdbcTemplate jdbcTemplate, Path outputDir) throws IOException {
        try(BufferedWriter writer = Files.newBufferedWriter(outputDir);
            CSVWriter csvWriter = new CSVWriter(writer)) {
            jdbcTemplate.query(sqlQuery, (ResultSet rs) -> {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Get the header
                String[] header = new String[columnCount];
                for(int i=1;i<=columnCount;i++){
                    header[i-1] = metaData.getColumnLabel(i);
                }
                csvWriter.writeNext(header);

                // Get all the rows
                while(rs.next()){
                    String[] row = new String[columnCount];
                    for(int i=1;i<=columnCount;i++){
                        Object value = rs.getObject(i);
                        row[i-1] = (value != null) ? value.toString() : "";
                    }
                    csvWriter.writeNext(row);
                }
                return null;
            });
            return outputDir.toString();
        } catch (Exception ex){
            log.error("Error while writing into CSV file: ", ex);
            throw new RuntimeException(ex);
        }
    }
}
