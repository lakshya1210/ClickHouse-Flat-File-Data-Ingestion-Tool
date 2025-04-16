package com.zeotap.integration.model;

import lombok.Data;

/**
 * Configuration class for ClickHouse database connection
 */
@Data
public class ClickHouseConfig {
    private String host;
    private int port;
    private String database;
    private String user;
    private String jwtToken;
    private boolean secure; // Whether to use HTTPS (8443/9440) or HTTP (8123/9000)
    
    /**
     * Constructs the JDBC URL for ClickHouse connection
     * @return JDBC URL string
     */
    public String getJdbcUrl() {
        String protocol = secure ? "https" : "http";
        // Add compress=0 parameter to disable compression and avoid LZ4 dependency issue
        return String.format("jdbc:clickhouse:%s://%s:%d/%s?compress=0", protocol, host, port, database);
    }
}