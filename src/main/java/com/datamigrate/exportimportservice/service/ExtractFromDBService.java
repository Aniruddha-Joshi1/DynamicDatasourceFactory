package com.datamigrate.exportimportservice.service;

import com.datamigrate.exportimportservice.model.PublishAndExtractFromDBRequestModel;
import com.datamigrate.exportimportservice.utils.ConnectionManagerUtils;
import com.datamigrate.exportimportservice.utils.ParquetOutputFileUtils;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
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
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class ExtractFromDBService {
    // Step1: Store the data from the DB
    // Step2: Create a directory to store the exported files (Store with unique file names)
    // Step3: Create a file and put the data extracted from the DB into the file. Headers will be the column names which are comma separated.
    @Value("${app.output.dir}")
    Path outputDir;

    private final ConnectionManagerUtils connectionManagerUtils;

    @Autowired
    public ExtractFromDBService(ConnectionManagerUtils connectionManagerUtils){
        this.connectionManagerUtils = connectionManagerUtils;
    }

    public String extractDataFromFile(PublishAndExtractFromDBRequestModel request) throws SQLException, IOException {
        if (request.getFileFormat() == null) {
            throw new IllegalArgumentException("File format must be provided");
        }
        ensureOutputDirExists();
        switch (request.getFileFormat()){
            case CSV: return extractDataFromCSVFile(request);
            case PARQUET: return extractDataFromParquetFile(request);
            default: throw new IllegalArgumentException("Unsupported file format");
        }
    }

    private String extractDataFromCSVFile(PublishAndExtractFromDBRequestModel request){
        String sqlQuery = buildSqlQuery(request);
        log.info("Extracting from the schema = '{}' and table = '{}'", request.getSchemaDetails().getSchema(),
                request.getSchemaDetails().getTableName());
        Path filePath = resolveOutputPath(request.getSchemaDetails().getTableName(), "csv");
        Connection connection = connectionManagerUtils.getConnection(request.getDatabaseConfig());
        try{
            JdbcTemplate jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            return extractDataFromDBIntoCsv(sqlQuery, jdbcTemplate, filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            connectionManagerUtils.closeConnection(connection);
        }
    }

    private String extractDataFromParquetFile(PublishAndExtractFromDBRequestModel request) throws IOException, SQLException {
        String sqlQuery = buildSqlQuery(request);
        log.info("Started extraction from the schema='{}' and table = '{}'", request.getSchemaDetails().getSchema(),
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
            // used to write data into a Parquet file
            ParquetWriter<Group> parquetWriter = ExampleParquetWriter
                    .builder(new ParquetOutputFileUtils(filePath))
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withType(schema)
                    .build();
            try(parquetWriter){
                // used to create row objects
                SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
                while(rs.next()){
                    // creates an empty row
                    Group group = groupFactory.newGroup();
                    // loops through the columns and adds the values to the group
                    for (int i = 1; i<= columnCount; i++){
                        appendValueToGroup(group, schema, i-1, rs, metaData.getColumnType(i));
                    }
                    parquetWriter.write(group);
                }
            }
            return filePath.toString();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to extract to Parquet file formate: " + e.getMessage(), e);
        } finally {
            connectionManagerUtils.closeConnection(connection);
        }
    }

    // appends a row in the row group. This function is called inside a for loop, so it appends all the rows in the writer
    private void appendValueToGroup(Group group, MessageType schema, int colIndex, ResultSet rs, int sqlType) throws SQLException {
        String fieldName = schema.getType(colIndex).getName();
        Object rawValue = rs.getObject(colIndex + 1);

        // Add debug logging
        log.debug("Field: {}, SQL Type: {}, Raw Value Class: {}",
                fieldName, sqlType, rawValue != null ? rawValue.getClass().getSimpleName() : "null");
        if (rawValue == null || rs.wasNull()) {
            return;
        }

        switch (sqlType) {
            // INT32 fields
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER ->
                    group.add(fieldName, ((Number) rawValue).intValue());

            // INT64 fields
            case Types.BIGINT ->
                    group.add(fieldName, ((Number) rawValue).longValue());

            // Float
            case Types.FLOAT, Types.REAL ->
                    group.add(fieldName, ((Number) rawValue).floatValue());

            // Double
            case Types.DOUBLE ->
                    group.add(fieldName, ((Number) rawValue).doubleValue());

            // DECIMAL / NUMERIC → BINARY (two's-complement bytes)
            // BigDecimal.unscaledValue().toByteArray() gives exactly the bytes
            // that the DECIMAL logical type expects.
            case Types.DECIMAL, Types.NUMERIC -> {
                BigDecimal bd = (rawValue instanceof BigDecimal b)
                        ? b
                        : new BigDecimal(rawValue.toString());
                byte[] bytes = bd.unscaledValue().toByteArray();
                group.add(fieldName, org.apache.parquet.io.api.Binary.fromConstantByteArray(bytes));
            }

            // Boolean
            case Types.BOOLEAN, Types.BIT ->
                    group.add(fieldName, (Boolean) rawValue);

            // String types → BINARY
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                 Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR ->
                    group.add(fieldName,
                            org.apache.parquet.io.api.Binary.fromString(rawValue.toString()));

            // DATE → INT32 (days since 1970-01-01)
            case Types.DATE -> {
                java.sql.Date date = rs.getDate(colIndex + 1);
                if (date != null) {
                    long days = TimeUnit.MILLISECONDS.toDays(date.getTime());
                    group.add(fieldName, (int) days);
                }
            }

            // TIME → INT32 (millis since midnight)
            case Types.TIME -> {
                java.sql.Time time = rs.getTime(colIndex + 1);
                if (time != null) {
                    // time.getTime() returns millis-since-midnight for java.sql.Time
                    group.add(fieldName, (int) time.getTime());
                }
            }

            // TIMESTAMP → INT64 (microseconds since epoch, UTC)
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
                Timestamp ts = rs.getTimestamp(colIndex + 1);
                if (ts != null) {
                    // Combine seconds-part and nanos-part to get full microseconds.
                    long micros = TimeUnit.SECONDS.toMicros(ts.getTime() / 1_000)
                            + TimeUnit.NANOSECONDS.toMicros(ts.getNanos() % 1_000_000_000L);
                    group.add(fieldName, micros);
                }
            }

            // Raw bytes → BINARY
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> {
                byte[] bytes = rs.getBytes(colIndex + 1);
                if (bytes != null) {
                    group.add(fieldName, org.apache.parquet.io.api.Binary.fromConstantByteArray(bytes));
                }
            }

            // Fallback: toString → UTF-8 BINARY
            default ->
                    group.add(fieldName,
                            org.apache.parquet.io.api.Binary.fromString(rawValue.toString()));
        }
    }

    // Parquet file needs a schema unlike the CSV, it takes sql type from the DB metadata and maps sql type to the parquet physical + logical equivalent
    private MessageType buildParquetSchema(ResultSetMetaData metaData, int columnCount) throws SQLException{
        List<Type> fields = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            int sqlType = metaData.getColumnType(i);
            int precision = metaData.getPrecision(i);
            int scale = metaData.getScale(i);

            Type field = buildParquetField(columnName, sqlType, precision, scale);
            fields.add(field);
        }
        String tableName = metaData.getTableName(1);
        return new MessageType(
                (tableName != null && !tableName.isBlank()) ? tableName : "export", fields
        );
    }

    // matches SQL types to parquet format (physical + logical type)
    private Type buildParquetField(String name, int sqlType, int precision, int scale) {
        return switch (sqlType) {

            // Integer types
            case Types.TINYINT ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                            .as(LogicalTypeAnnotation.intType(8, true))
                            .named(name);

            case Types.SMALLINT ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                            .as(LogicalTypeAnnotation.intType(16, true))
                            .named(name);

            case Types.INTEGER ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                            .as(LogicalTypeAnnotation.intType(32, true))
                            .named(name);

            case Types.BIGINT ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                            .as(LogicalTypeAnnotation.intType(64, true))
                            .named(name);

            // Floating-point types
            case Types.FLOAT, Types.REAL ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
                            .named(name);

            case Types.DOUBLE ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                            .named(name);

            // Exact numeric — keep full precision via DECIMAL logical type
            // Physical storage: BINARY (variable-length two's-complement bytes).
            // This avoids the precision loss that would come from casting to DOUBLE.
            case Types.DECIMAL, Types.NUMERIC -> {
                int p = (precision > 0) ? precision : 38;   // sane default
                int s = Math.max(scale, 0);
                yield org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.decimalType(s, p))
                        .named(name);
            }

            // Boolean
            case Types.BOOLEAN, Types.BIT ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                            .named(name);

            // String types — annotated as UTF-8 STRING
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                 Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.stringType())
                            .named(name);

            // Temporal types
            // DATE → INT32, days since Unix epoch (1970-01-01).
            case Types.DATE ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                            .as(LogicalTypeAnnotation.dateType())
                            .named(name);

            // TIME → INT32, milliseconds since midnight (no timezone).
            case Types.TIME ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                            .as(LogicalTypeAnnotation.timeType(false,
                                    LogicalTypeAnnotation.TimeUnit.MILLIS))
                            .named(name);

            // TIMESTAMP / TIMESTAMP_WITH_TIMEZONE → INT64, microseconds since epoch, UTC-normalised.
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                            .as(LogicalTypeAnnotation.timestampType(true,
                                    LogicalTypeAnnotation.TimeUnit.MICROS))
                            .named(name);

            // Raw bytes — no logical annotation
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                            .named(name);

            // Fallback: everything else as a UTF-8 string
            default ->
                    org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.stringType())
                            .named(name);
        };
    }

    private String buildSqlQuery(PublishAndExtractFromDBRequestModel request) {
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

    private String extractDataFromDBIntoCsv(String sqlQuery, JdbcTemplate jdbcTemplate, Path outputDir) throws IOException {
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

    private void ensureOutputDirExists() throws IOException {
        Path dir = Paths.get(outputDir.toUri());
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
            log.info("Created output directory: {}", dir);
        }
    }
}
