package com.datamigrate.exportimportservice.service;

import com.datamigrate.exportimportservice.enums.FileFormat;
import com.datamigrate.exportimportservice.model.ImportExportRequestModel;
import com.datamigrate.exportimportservice.utils.ConnectionManagerUtils;
import com.datamigrate.exportimportservice.utils.ParquetOutputFileUtils;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class ExportService {
    // Step1: Store the data from the DB
    // Step2: Create a directory to store the exported files (Store with unique file names)
    // Step3: Create a file and put the data extracted from the DB into the file. Headers will be the column names which are comma separated.
    @Value("${app.output.dir}")
    Path outputDir;

    private final ConnectionManagerUtils connectionManagerUtils;

    @Autowired
    public ExportService(ConnectionManagerUtils connectionManagerUtils){
        this.connectionManagerUtils = connectionManagerUtils;
    }

    public String exportFile (ImportExportRequestModel request) throws SQLException, IOException {
        if (request.getFileFormat() == null) {
            throw new IllegalArgumentException("File format must be provided");
        }
        if (request.getFileFormat() == FileFormat.CSV) {
            return exportToCsv(request);
        } else if (request.getFileFormat() == FileFormat.PARQUET) {
            return exportToParquet(request);
        } else {
            throw new IllegalArgumentException("Unsupported file format");
        }
    }

    public String exportToCsv(ImportExportRequestModel request){
        String sqlQuery = buildSqlQuery(request);
        log.info("Starting export from the schema = '{}' and table = '{}'", request.getSchemaDetails().getSchema(),
                request.getSchemaDetails().getTableName());
        Path filePath = resolveOutputPath(request.getSchemaDetails().getTableName(), "csv");
        Connection connection = connectionManagerUtils.getConnection(request.getDatabaseConfig());
        try{
            if(!Files.exists(Paths.get(outputDir.toUri()))){
                Files.createDirectories(outputDir);
            }
            JdbcTemplate jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            return retrieveAndExportToCsv(sqlQuery, jdbcTemplate, filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            connectionManagerUtils.closeConnection(connection);
        }
    }

    public String exportToParquet(ImportExportRequestModel request) throws IOException, SQLException {
        String sqlQuery = buildSqlQuery(request);
        log.info("Starting export from the schema='{}' and table = '{}'", request.getSchemaDetails().getSchema(),
                request.getSchemaDetails().getTableName());
        Path filePath = resolveOutputPath(request.getSchemaDetails().getTableName(), "parquet");
        Connection connection = connectionManagerUtils.getConnection(request.getDatabaseConfig());
        try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sqlQuery)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Build parquet schema from result set metadata, preserving SQL columns
            MessageType schema = buildParquetSchema(metaData, columnCount);

            // Group - one row of data
            ParquetWriter<Group> parquetWriter = ExampleParquetWriter
                    .builder(new ParquetOutputFileUtils(filePath))
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withType(schema)
                    .build();

            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
            try(parquetWriter){
                while(rs.next()){
                    Group group = groupFactory.newGroup();
                    for (int i = 1; i<= columnCount; i++){
                        Object value = rs.getObject(i);
                        if (value != null) {
                            appendValueToGroup(group, schema.getType(i-1).getName(), value);
                        }
                    }
                    parquetWriter.write(group);
                }
            }
            return filePath.toString();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to export to Parquet: " + e.getMessage(), e);
        } finally {
            connectionManagerUtils.closeConnection(connection);
        }
    }

    private void appendValueToGroup(Group group, String fieldName, Object value) {
        switch (value) {
            case Integer v -> group.add(fieldName, v);
            case Long v    -> group.add(fieldName, v);
            case Float v   -> group.add(fieldName, v);
            case Double v  -> group.add(fieldName, v);
            case Boolean v -> group.add(fieldName, v);
            default        -> group.add(fieldName, value.toString());
        }
    }

    // Parquet file needs a schema unlike the CSV
    private MessageType buildParquetSchema(ResultSetMetaData metaData, int columnCount) throws SQLException{
        List<Type> fields = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            int sqlType = metaData.getColumnType(i);

            PrimitiveType.PrimitiveTypeName parquetType = switch (sqlType) {
                case Types.INTEGER, Types.SMALLINT, Types.TINYINT -> PrimitiveType.PrimitiveTypeName.INT32;
                case Types.BIGINT -> PrimitiveType.PrimitiveTypeName.INT64;
                case Types.FLOAT, Types.REAL -> PrimitiveType.PrimitiveTypeName.FLOAT;
                case Types.DOUBLE, Types.DECIMAL, Types.NUMERIC -> PrimitiveType.PrimitiveTypeName.DOUBLE;
                case Types.BOOLEAN, Types.BIT -> PrimitiveType.PrimitiveTypeName.BOOLEAN;
                default -> PrimitiveType.PrimitiveTypeName.BINARY;
            };

            fields.add(org.apache.parquet.schema.Types.optional(parquetType).named(columnName));
        }
        String tableName = metaData.getTableName(1);
        return new MessageType(
                (tableName != null && !tableName.isBlank()) ? tableName : "export", fields
        );
    }

    private String buildSqlQuery(ImportExportRequestModel request) {
        StringBuilder query = new StringBuilder("SELECT * FROM ");
        query.append(request.getSchemaDetails().getSchema());
        query.append(".");
        query.append(request.getSchemaDetails().getTableName());
        return query.toString();
    }

    private Path resolveOutputPath(String tableName, String extension){
        String fileName = tableName + "_" + System.currentTimeMillis() + "." + extension;
        return Paths.get(outputDir.toUri()).resolve(fileName);
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
