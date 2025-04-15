package com.zeotap.integration.controller;

import com.zeotap.integration.model.ClickHouseConfig;
import com.zeotap.integration.model.ColumnMetadata;
import com.zeotap.integration.model.FlatFileConfig;
import com.zeotap.integration.model.IngestionRequest;
import com.zeotap.integration.service.IntegrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST controller for data ingestion operations
 */
@RestController
@RequestMapping("/api/integration")
@RequiredArgsConstructor
@Slf4j
public class IntegrationController {

    private final IntegrationService integrationService;

    /**
     * Tests connection to ClickHouse
     *
     * @param config ClickHouse connection configuration
     * @return Success/failure response
     */
    @PostMapping("/clickhouse/test-connection")
    public ResponseEntity<Map<String, Object>> testClickHouseConnection(@RequestBody ClickHouseConfig config) {
        Map<String, Object> response = new HashMap<>();
        try {
            integrationService.getClickHouseTables(config);
            response.put("success", true);
            response.put("message", "Connection successful");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error connecting to ClickHouse", e);
            response.put("success", false);
            response.put("message", "Connection failed: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Gets the list of tables from ClickHouse
     *
     * @param config ClickHouse connection configuration
     * @return List of table names
     */
    @PostMapping("/clickhouse/tables")
    public ResponseEntity<Map<String, Object>> getClickHouseTables(@RequestBody ClickHouseConfig config) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<String> tables = integrationService.getClickHouseTables(config);
            response.put("success", true);
            response.put("tables", tables);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting ClickHouse tables", e);
            response.put("success", false);
            response.put("message", "Failed to get tables: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Gets the schema of a ClickHouse table
     *
     * @param config    ClickHouse connection configuration
     * @param tableName Table name
     * @return List of column metadata
     */
    @PostMapping("/clickhouse/schema")
    public ResponseEntity<Map<String, Object>> getClickHouseSchema(
            @RequestBody ClickHouseConfig config,
            @RequestParam String tableName) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<ColumnMetadata> columns = integrationService.getClickHouseTableSchema(config, tableName);
            response.put("success", true);
            response.put("columns", columns);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting ClickHouse schema", e);
            response.put("success", false);
            response.put("message", "Failed to get schema: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Gets the schema of a flat file
     *
     * @param config Flat file configuration
     * @return List of column metadata
     */
    @PostMapping("/flatfile/schema")
    public ResponseEntity<Map<String, Object>> getFlatFileSchema(@RequestBody FlatFileConfig config) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<ColumnMetadata> columns = integrationService.getFlatFileSchema(config);
            response.put("success", true);
            response.put("columns", columns);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting flat file schema", e);
            response.put("success", false);
            response.put("message", "Failed to get schema: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Previews data from ClickHouse
     *
     * @param request Ingestion request with source configuration and column selection
     * @return Preview data
     */
    @PostMapping("/clickhouse/preview")
    public ResponseEntity<Map<String, Object>> previewClickHouseData(@RequestBody IngestionRequest request) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<Map<String, Object>> data;
            if (request.getAdditionalTables() != null && !request.getAdditionalTables().isEmpty() &&
                request.getJoinCondition() != null && !request.getJoinCondition().isEmpty()) {
                // Use JOIN preview if multiple tables are selected
                data = integrationService.previewClickHouseJoinData(
                        request.getClickHouseConfig(),
                        request.getTableName(),
                        request.getAdditionalTables(),
                        request.getJoinCondition(),
                        request.getSelectedColumns(),
                        100); // Preview limit
            } else {
                // Use simple preview for single table
                data = integrationService.previewClickHouseData(
                        request.getClickHouseConfig(),
                        request.getTableName(),
                        request.getSelectedColumns(),
                        100); // Preview limit
            }
            response.put("success", true);
            response.put("data", data);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error previewing ClickHouse data", e);
            response.put("success", false);
            response.put("message", "Failed to preview data: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Previews data from a flat file
     *
     * @param request Ingestion request with source configuration and column selection
     * @return Preview data
     */
    @PostMapping("/flatfile/preview")
    public ResponseEntity<Map<String, Object>> previewFlatFileData(@RequestBody IngestionRequest request) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<Map<String, Object>> data = integrationService.previewFlatFileData(
                    request.getFlatFileConfig(),
                    request.getSelectedColumns(),
                    100); // Preview limit
            response.put("success", true);
            response.put("data", data);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error previewing flat file data", e);
            response.put("success", false);
            response.put("message", "Failed to preview data: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Executes the data ingestion process
     *
     * @param request Ingestion request with source, target, and column selection
     * @return Ingestion result with record count
     */
    @PostMapping("/execute")
    public ResponseEntity<Map<String, Object>> executeIngestion(@RequestBody IngestionRequest request) {
        Map<String, Object> response = new HashMap<>();
        try {
            int recordCount = integrationService.executeIngestion(request);
            response.put("success", true);
            response.put("recordCount", recordCount);
            response.put("message", "Ingestion completed successfully");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error executing ingestion", e);
            response.put("success", false);
            response.put("message", "Ingestion failed: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
}