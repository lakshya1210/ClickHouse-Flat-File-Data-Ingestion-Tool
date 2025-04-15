package com.zeotap.integration.service;

import com.zeotap.integration.model.ClickHouseConfig;
import com.zeotap.integration.model.ColumnMetadata;
import com.zeotap.integration.model.FlatFileConfig;
import com.zeotap.integration.model.IngestionRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Service for coordinating data ingestion between ClickHouse and Flat File
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IntegrationService {

    private final ClickHouseService clickHouseService;
    private final FlatFileService flatFileService;

    /**
     * Executes the data ingestion process based on the request
     *
     * @param request Ingestion request with source, target, and column selection
     * @return Number of records processed
     * @throws Exception if ingestion fails
     */
    public int executeIngestion(IngestionRequest request) throws Exception {
        log.info("Starting ingestion from {} to {}", request.getSourceType(), request.getTargetType());
        
        // Validate request
        validateRequest(request);
        
        // Execute ingestion based on source and target types
        if ("clickhouse".equalsIgnoreCase(request.getSourceType()) && 
            "flatfile".equalsIgnoreCase(request.getTargetType())) {
            return ingestFromClickHouseToFlatFile(request);
        } else if ("flatfile".equalsIgnoreCase(request.getSourceType()) && 
                   "clickhouse".equalsIgnoreCase(request.getTargetType())) {
            return ingestFromFlatFileToClickHouse(request);
        } else {
            throw new IllegalArgumentException("Unsupported source or target type");
        }
    }

    /**
     * Validates the ingestion request
     *
     * @param request Ingestion request to validate
     * @throws IllegalArgumentException if request is invalid
     */
    private void validateRequest(IngestionRequest request) {
        if (request.getSourceType() == null || request.getTargetType() == null) {
            throw new IllegalArgumentException("Source and target types must be specified");
        }
        
        if ("clickhouse".equalsIgnoreCase(request.getSourceType()) && request.getClickHouseConfig() == null) {
            throw new IllegalArgumentException("ClickHouse configuration must be provided when ClickHouse is the source");
        }
        
        if ("flatfile".equalsIgnoreCase(request.getSourceType()) && request.getFlatFileConfig() == null) {
            throw new IllegalArgumentException("Flat File configuration must be provided when Flat File is the source");
        }
        
        if ("clickhouse".equalsIgnoreCase(request.getTargetType()) && request.getClickHouseConfig() == null) {
            throw new IllegalArgumentException("ClickHouse configuration must be provided when ClickHouse is the target");
        }
        
        if ("flatfile".equalsIgnoreCase(request.getTargetType()) && request.getFlatFileConfig() == null) {
            throw new IllegalArgumentException("Flat File configuration must be provided when Flat File is the target");
        }
        
        if (request.getSelectedColumns() == null || request.getSelectedColumns().isEmpty()) {
            throw new IllegalArgumentException("At least one column must be selected for ingestion");
        }
    }

    /**
     * Ingests data from ClickHouse to Flat File
     *
     * @param request Ingestion request
     * @return Number of records processed
     * @throws SQLException if database operation fails
     * @throws IOException  if file operation fails
     */
    private int ingestFromClickHouseToFlatFile(IngestionRequest request) throws SQLException, IOException {
        log.info("Ingesting data from ClickHouse to Flat File");
        
        // Connect to ClickHouse
        try (Connection connection = clickHouseService.connect(request.getClickHouseConfig())) {
            // Create a data handler for writing to flat file
            ClickHouseService.DataHandler flatFileHandler = 
                    flatFileService.createFlatFileDataHandler(request.getFlatFileConfig(), request.getSelectedColumns());
            
            // Transfer data
            int recordCount;
            if (request.getAdditionalTables() != null && !request.getAdditionalTables().isEmpty() && 
                request.getJoinCondition() != null && !request.getJoinCondition().isEmpty()) {
                // Use JOIN query if multiple tables are selected
                recordCount = clickHouseService.transferJoinDataFromClickHouse(
                        connection, 
                        request.getTableName(), 
                        request.getAdditionalTables(), 
                        request.getJoinCondition(), 
                        request.getSelectedColumns(), 
                        flatFileHandler);
            } else {
                // Use simple query for single table
                recordCount = clickHouseService.transferDataFromClickHouse(
                        connection, 
                        request.getTableName(), 
                        request.getSelectedColumns(), 
                        flatFileHandler);
            }
            
            log.info("Ingestion completed: {} records transferred from ClickHouse to Flat File", recordCount);
            return recordCount;
        }
    }

    /**
     * Ingests data from Flat File to ClickHouse
     *
     * @param request Ingestion request
     * @return Number of records processed
     * @throws SQLException if database operation fails
     * @throws IOException  if file operation fails
     */
    private int ingestFromFlatFileToClickHouse(IngestionRequest request) throws SQLException, IOException {
        log.info("Ingesting data from Flat File to ClickHouse");
        
        // Connect to ClickHouse
        try (Connection connection = clickHouseService.connect(request.getClickHouseConfig())) {
            // Create target table in ClickHouse if it doesn't exist
            clickHouseService.createTable(connection, request.getTargetTableName(), request.getSelectedColumns());
            
            // Read data from flat file
            List<Map<String, Object>> data = flatFileService.readData(
                    request.getFlatFileConfig(), 
                    request.getSelectedColumns(), 
                    0); // No limit for full ingestion
            
            // Insert data into ClickHouse
            int recordCount = clickHouseService.insertData(
                    connection, 
                    request.getTargetTableName(), 
                    request.getSelectedColumns(), 
                    data);
            
            log.info("Ingestion completed: {} records transferred from Flat File to ClickHouse", recordCount);
            return recordCount;
        }
    }

    /**
     * Fetches the list of tables from ClickHouse
     *
     * @param config ClickHouse configuration
     * @return List of table names
     * @throws SQLException if database operation fails
     */
    public List<String> getClickHouseTables(ClickHouseConfig config) throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.getTables(connection);
        }
    }

    /**
     * Fetches the schema of a ClickHouse table
     *
     * @param config    ClickHouse configuration
     * @param tableName Table name
     * @return List of column metadata
     * @throws SQLException if database operation fails
     */
    public List<ColumnMetadata> getClickHouseTableSchema(ClickHouseConfig config, String tableName) throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.getTableSchema(connection, tableName);
        }
    }

    /**
     * Fetches the schema of a flat file
     *
     * @param config Flat file configuration
     * @return List of column metadata
     * @throws IOException if file operation fails
     */
    public List<ColumnMetadata> getFlatFileSchema(FlatFileConfig config) throws IOException {
        return flatFileService.readFileSchema(config);
    }

    /**
     * Previews data from ClickHouse
     *
     * @param config    ClickHouse configuration
     * @param tableName Table name
     * @param columns   List of columns to preview
     * @param limit     Maximum number of rows to preview
     * @return List of data rows
     * @throws SQLException if database operation fails
     */
    public List<Map<String, Object>> previewClickHouseData(ClickHouseConfig config, String tableName, 
                                                        List<ColumnMetadata> columns, int limit) throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.previewData(connection, tableName, columns, limit);
        }
    }

    /**
     * Previews data from ClickHouse with JOIN
     *
     * @param config           ClickHouse configuration
     * @param mainTable        Main table name
     * @param additionalTables Additional tables for JOIN
     * @param joinCondition    JOIN condition
     * @param columns          List of columns to preview
     * @param limit            Maximum number of rows to preview
     * @return List of data rows
     * @throws SQLException if database operation fails
     */
    public List<Map<String, Object>> previewClickHouseJoinData(ClickHouseConfig config, String mainTable, 
                                                            List<String> additionalTables, String joinCondition, 
                                                            List<ColumnMetadata> columns, int limit) throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.previewJoinData(connection, mainTable, additionalTables, joinCondition, columns, limit);
        }
    }

    /**
     * Previews data from a flat file
     *
     * @param config  Flat file configuration
     * @param columns List of columns to preview
     * @param limit   Maximum number of rows to preview
     * @return List of data rows
     * @throws IOException if file operation fails
     */
    public List<Map<String, Object>> previewFlatFileData(FlatFileConfig config, List<ColumnMetadata> columns, 
                                                      int limit) throws IOException {
        return flatFileService.readData(config, columns, limit);
    }
}