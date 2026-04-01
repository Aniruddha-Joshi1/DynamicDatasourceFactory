package com.datamigrate.exportimportservice.service;

import com.datamigrate.exportimportservice.model.PublishAndExtractFromDBRequestModel;
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
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class PublishIntoDBService {
    // Step1: Read the file and store the data in a data structure
    // Step2: Write an insert query with the data
    // Step3: Execute the insert query using the jdbc connection to insert data into DB
    private final ConnectionManagerUtils connectionManagerUtils;

    @Autowired
    public PublishIntoDBService(ConnectionManagerUtils connectionManagerUtils){
        this.connectionManagerUtils = connectionManagerUtils;
    }

    public void publishDataIntoDB(PublishAndExtractFromDBRequestModel request) throws CsvValidationException, IOException {
        String filePath = request.getFilePath();
        Path fullPath = Paths.get(filePath);
        String fileName = fullPath.getFileName().toString();
        if (fileName == null){
            throw new IllegalArgumentException("File name is missing");
        }
        fileName = fileName.toLowerCase();
        if(fileName.endsWith(".csv")){
            log.info("Import request received for CSV file = '{}' into table ='{}'", fileName, request.getSchemaDetails().getTableName());
            publishDataIntoDBFromCSVFile(fileName, request);
        } else if (fileName.endsWith(".parquet")) {
            log.info("Import request received for Parquet file = '{}' into table ='{}'", fileName, request.getSchemaDetails().getTableName());
            publishDataIntoDBFromParquetFile(fileName, request);
        } else {
            throw new IllegalArgumentException("Unsupported file type. Currently only CSV and PARQUET file are supported");
        }
    }

    public void publishDataIntoDBFromCSVFile(String fileName, PublishAndExtractFromDBRequestModel importRequest) throws CsvValidationException, IOException {
        log.info("Starting import for the file='{}' and it will be imported into table = '{}'", fileName,
                importRequest.getSchemaDetails().getTableName());
        Path filePath = Paths.get(importRequest.getFilePath());
        Connection connection = connectionManagerUtils.getConnection(importRequest.getDatabaseConfig());
        try {
            JdbcTemplate jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            readCSVAndPublishIntoDB(filePath ,jdbc, importRequest);
        } finally {
            connectionManagerUtils.closeConnection(connection);
        }
    }

    public void publishDataIntoDBFromParquetFile(String fileName, PublishAndExtractFromDBRequestModel importRequest) throws IOException{
        log.info("Starting Parquet import for the file = '{}' into table = '{}'", fileName, importRequest.getSchemaDetails().getTableName());
        Path filePath = Paths.get(importRequest.getFilePath());
        if (!Files.exists(filePath)) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }
        String schemaName = importRequest.getSchemaDetails().getSchema();
        String tableName  = importRequest.getSchemaDetails().getTableName();

        try{
            Connection connection = connectionManagerUtils.getConnection(importRequest.getDatabaseConfig());
            try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInputFileUtils(filePath))) {
                // gets the schema
                MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                Set<String> autoIncrementCols = getAutoIncrementColumns(connection, schemaName, tableName);
                Map<Integer, String> insertableFields = new LinkedHashMap<>();
                for (int i = 0; i < schema.getFieldCount(); i++) {
                    String colName = schema.getType(i).getName();
                    if (!autoIncrementCols.contains(colName.toLowerCase())) {
                        insertableFields.put(i, colName);
                    }
                }
                String[] columns = insertableFields.values().toArray(String[]::new);
                String insertSql = buildInsertSql(extractedTableName(importRequest), columns);
                int batchSize = importRequest.getBatchSize();
                int[] sqlTypes = deriveJdbcTypesForFields(schema, insertableFields);
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
                        Object[] fullRow  = extractRowFromGroup(group, schema);
                        Object[] filteredRow = new Object[insertableFields.size()];
                        int j = 0;
                        for (int parquetIndex : insertableFields.keySet()) {
                            filteredRow[j++] = fullRow[parquetIndex];
                        }
                        batch.add(filteredRow);
                        if (batch.size() == batchSize) {
                            insertBatchIntoDB(jdbc, insertSql, batch, sqlTypes);
                            batch.clear();
                        }
                    }
                }
                if (!batch.isEmpty()){
                    insertBatchIntoDB(jdbc, insertSql, batch, sqlTypes);
                }
            } finally {
                connectionManagerUtils.closeConnection(connection);
            }
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> getAutoIncrementColumns(Connection connection,
                                                String schemaName,
                                                String tableName) throws SQLException {
        Set<String> autoColumns = new HashSet<>();
        DatabaseMetaData meta   = connection.getMetaData();

        // catalog = the actual database name (from the live connection).
        // For SQL Server this is e.g. "master" or "mydb" — NOT the schema ("dbo").
        // For MySQL, catalog IS the database name, and schemaPattern must be null.
        // For PostgreSQL and Oracle, catalog is typically null or the DB name,
        // and schemaPattern is what selects the namespace ("public", "dbo", etc.).
        String catalog = connection.getCatalog();  // never "dbo" — always the real DB name

        autoColumns = fetchAutoIncrementColumns(meta, catalog, schemaName, tableName);

        // If nothing came back, the driver may be case-sensitive in metadata calls.
        // Retry with upper-case and lower-case table/schema name variants.
        if (autoColumns.isEmpty()) {
            autoColumns = fetchAutoIncrementColumns(meta, catalog,
                    schemaName == null ? null : schemaName.toUpperCase(),
                    tableName.toUpperCase());
        }
        if (autoColumns.isEmpty()) {
            autoColumns = fetchAutoIncrementColumns(meta, catalog,
                    schemaName == null ? null : schemaName.toLowerCase(),
                    tableName.toLowerCase());
        }

        if (!autoColumns.isEmpty()) {
            log.info("Auto-increment columns detected in '{}.{}' — will be excluded from INSERT: {}",
                    schemaName, tableName, autoColumns);
        }
        return autoColumns;
    }

    private Set<String> fetchAutoIncrementColumns(DatabaseMetaData meta,
                                                  String catalog,
                                                  String schemaPattern,
                                                  String tableName) throws SQLException {
        Set<String> result = new HashSet<>();
        try (ResultSet rs = meta.getColumns(catalog, schemaPattern, tableName, null)) {
            while (rs.next()) {
                if ("YES".equalsIgnoreCase(rs.getString("IS_AUTOINCREMENT"))) {
                    result.add(rs.getString("COLUMN_NAME").toLowerCase());
                }
            }
        }
        return result;
    }

    // extracts the complete row values
    private Object[] extractRowFromGroup(Group group, MessageType schema) {
        int fieldCount = schema.getFieldCount();
        Object[] row   = new Object[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            // Null check — field may be optional and absent in this row
            if (group.getFieldRepetitionCount(i) == 0) {
                row[i] = null;
                continue;
            }

            // Map Parquet primitive type → Java type
            Type field = schema.getType(i);
            LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();
            PrimitiveType.PrimitiveTypeName physicalType = field.asPrimitiveType().getPrimitiveTypeName();
            row[i] = convertParquetValue(group, i, physicalType, annotation);
        }
        return row;
    }

    private Object convertParquetValue(Group group, int fieldIndex,
                                       PrimitiveType.PrimitiveTypeName physical,
                                       LogicalTypeAnnotation annotation) {
        // ── Logical annotation takes priority ────────────────────────────────
        if (annotation != null) {
            return switch (annotation) {

                // DATE: INT32 physical, value = days since 1970-01-01
                case LogicalTypeAnnotation.DateLogicalTypeAnnotation d -> {
                    int days = group.getInteger(fieldIndex, 0);
                    yield new Date(TimeUnit.DAYS.toMillis(days));
                }

                // TIME: INT32 (MILLIS) or INT64 (MICROS/NANOS) physical
                case LogicalTypeAnnotation.TimeLogicalTypeAnnotation t -> {
                    yield switch (t.getUnit()) {
                        case MILLIS -> {
                            int millis = group.getInteger(fieldIndex, 0);
                            yield new Time(millis);
                        }
                        case MICROS -> {
                            long micros = group.getLong(fieldIndex, 0);
                            yield new Time(TimeUnit.MICROSECONDS.toMillis(micros));
                        }
                        case NANOS -> {
                            long nanos = group.getLong(fieldIndex, 0);
                            yield new Time(TimeUnit.NANOSECONDS.toMillis(nanos));
                        }
                    };
                }

                // TIMESTAMP: INT64 physical, value = micros (or millis/nanos) since epoch
                case LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts -> {
                    yield switch (ts.getUnit()) {
                        case MILLIS -> {
                            long millis = group.getLong(fieldIndex, 0);
                            yield new Timestamp(millis);
                        }
                        case MICROS -> {
                            long micros = group.getLong(fieldIndex, 0);
                            long epochMillis = TimeUnit.MICROSECONDS.toMillis(micros);
                            // Preserve the sub-millisecond part as nanos on the Timestamp
                            int subMilliNanos = (int) TimeUnit.MICROSECONDS.toNanos(micros % 1_000);
                            Timestamp stamp   = new Timestamp(epochMillis);
                            // Timestamp.nanos holds the full nanosecond-of-second field.
                            // setNanos() resets it, so we must fold in the millis portion.
                            stamp.setNanos((int) (TimeUnit.MILLISECONDS.toNanos(epochMillis % 1_000))
                                    + subMilliNanos);
                            yield stamp;
                        }
                        case NANOS -> {
                            long nanos      = group.getLong(fieldIndex, 0);
                            long epochMillis = TimeUnit.NANOSECONDS.toMillis(nanos);
                            Timestamp stamp  = new Timestamp(epochMillis);
                            stamp.setNanos((int) (nanos % 1_000_000_000L));
                            yield stamp;
                        }
                    };
                }

                // DECIMAL: BINARY physical, value = two's-complement unscaled bytes
                case LogicalTypeAnnotation.DecimalLogicalTypeAnnotation dec -> {
                    Binary binary   = group.getBinary(fieldIndex, 0);
                    BigInteger unscaled = new BigInteger(binary.getBytes());
                    yield new BigDecimal(unscaled, dec.getScale());
                }

                // STRING, JSON, ENUM, BSON, UUID — all stored as UTF-8 BINARY
                case LogicalTypeAnnotation.StringLogicalTypeAnnotation ignored ->
                        group.getString(fieldIndex, 0);

                // INT annotation — physical is INT32 or INT64
                case LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation -> {
                    if (intAnnotation.getBitWidth() == 64) {
                        yield group.getLong(fieldIndex, 0);
                    }
                    // 8, 16, 32-bit — all stored as INT32
                    yield group.getInteger(fieldIndex, 0);
                }

                // Anything else with an annotation we don't explicitly handle:
                // fall through to physical-type dispatch below.
                default -> {
                    if (annotation instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation
                            || annotation instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation
                            || annotation instanceof LogicalTypeAnnotation.BsonLogicalTypeAnnotation
                            || annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
                        yield group.getString(fieldIndex, 0);
                    }
                    yield convertByPhysicalType(group, fieldIndex, physical);
                }
            };
        }

        // ── No annotation — dispatch on physical type only ───────────────────
        return convertByPhysicalType(group, fieldIndex, physical);
    }

    private Object convertByPhysicalType(Group group, int fieldIndex,
                                         PrimitiveType.PrimitiveTypeName physical) {
        return switch (physical) {
            case INT32 -> group.getInteger(fieldIndex, 0);
            case INT64, INT96 -> group.getLong(fieldIndex, 0);
            case FLOAT -> group.getFloat(fieldIndex, 0);
            case DOUBLE -> group.getDouble(fieldIndex, 0);
            case BOOLEAN -> group.getBoolean(fieldIndex, 0);
            // Unannotated BINARY = raw bytes (VARBINARY / BYTEA / BLOB)
            case BINARY, FIXED_LEN_BYTE_ARRAY -> group.getBinary(fieldIndex, 0).getBytes();
        };
    }

    private void readCSVAndPublishIntoDB(Path filePath, JdbcTemplate jdbc, PublishAndExtractFromDBRequestModel importRequest) throws IOException, CsvValidationException {
        try (CSVReader csvReader = new CSVReader(
                new BufferedReader(
                        new InputStreamReader(Files.newInputStream(filePath), StandardCharsets.UTF_8)
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
                    insertBatchIntoDB(jdbc, insertSqlQuery, batch, null);
                    batch.clear();
                }
            }

            if(!batch.isEmpty()){
                insertBatchIntoDB(jdbc, insertSqlQuery, batch, null);
            }
        }
    }

    private void insertBatchIntoDB(JdbcTemplate jdbc, String sqlQuery, List<Object[]> batchData, int[] sqlTypes){
        try{
            jdbc.batchUpdate(sqlQuery, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int rowIndex) throws SQLException {
                    Object[] row = batchData.get(rowIndex);
                    for(int col = 0; col < row.length; col++){
                        if (row[col] == null) {
                            int sqlType = (sqlTypes != null) ? sqlTypes[col] : Types.NVARCHAR;
                            ps.setNull(col + 1, sqlType);
                        } else {
                            ps.setObject(col + 1, row[col]);
                        }
                    }
                }

                @Override
                public int getBatchSize() {
                    return batchData.size();
                }
            });
        } catch (Exception e){
            log.error("Batch insert failed (batch size = {}): {}", batchData.size(), e.getMessage());
        }
    }

    private int[] deriveJdbcTypesForFields(MessageType parquetSchema,
                                           Map<Integer, String> insertableFields) {
        int[] types = new int[insertableFields.size()];
        int j = 0;
        for (int parquetIndex : insertableFields.keySet()) {
            Type field = parquetSchema.getType(parquetIndex);
            LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();

            if (annotation != null) {
                types[j] = switch (annotation) {
                    case LogicalTypeAnnotation.DateLogicalTypeAnnotation      ignored -> Types.DATE;
                    case LogicalTypeAnnotation.TimeLogicalTypeAnnotation      ignored -> Types.TIME;
                    case LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ignored -> Types.TIMESTAMP;
                    case LogicalTypeAnnotation.DecimalLogicalTypeAnnotation   ignored -> Types.DECIMAL;
                    case LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnn ->
                            intAnn.getBitWidth() == 64 ? Types.BIGINT : Types.INTEGER;
                    default -> Types.NVARCHAR;
                };
            } else {
                types[j] = switch (field.asPrimitiveType().getPrimitiveTypeName()) {
                    case INT32                    -> Types.INTEGER;
                    case INT64, INT96             -> Types.BIGINT;
                    case FLOAT                    -> Types.FLOAT;
                    case DOUBLE                   -> Types.DOUBLE;
                    case BOOLEAN                  -> Types.BOOLEAN;
                    case BINARY, FIXED_LEN_BYTE_ARRAY -> Types.VARBINARY;
                };
            }
            j++;
        }
        return types;
    }

    private String buildInsertSql(String tableName, String[] headers){
        String columns = String.join(", ", headers);
        String placeholders = "?, ".repeat(headers.length);
        placeholders = placeholders.substring(0, placeholders.length()-2);
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    private String extractedTableName(PublishAndExtractFromDBRequestModel importRequest){
        String schema = importRequest.getSchemaDetails().getSchema();
        String table = importRequest.getSchemaDetails().getTableName();
        if (schema != null && !schema.isBlank()){
            return schema + "." + table;
        }
        return table;
    }
}
