package com.zeotap.integration.model;

import java.util.List;

import lombok.Data;

/**
 * Represents a complete data ingestion request with source, target, and column selection
 */
@Data
public class IngestionRequest {
    // Source type: "clickhouse" or "flatfile"
    private String sourceType;
    
    // Target type: "clickhouse" or "flatfile"
    private String targetType;
    
    // ClickHouse configuration (used when ClickHouse is source or target)
    private ClickHouseConfig clickHouseConfig;
    
    // Flat File configuration (used when Flat File is source or target)
    private FlatFileConfig flatFileConfig;
    
    // Selected table name when ClickHouse is source
    private String tableName;
    
    // For bonus requirement: multiple table join
    private List<String> additionalTables;
    private String joinCondition;
    
    // Selected columns for ingestion
    private List<ColumnMetadata> selectedColumns;
    
    // Target table name when ClickHouse is target
    private String targetTableName;
}