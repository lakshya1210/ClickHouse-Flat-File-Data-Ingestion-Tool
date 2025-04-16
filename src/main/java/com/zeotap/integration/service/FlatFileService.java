package com.zeotap.integration.service;

import com.zeotap.integration.model.ColumnMetadata;
import com.zeotap.integration.model.FlatFileConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for handling Flat File operations
 */
@Service
@Slf4j
public class FlatFileService {

    /**
     * Resolves a file path or URL to a local file path
     * 
     * @param filePathOrUrl File path or URL string
     * @return Local file path
     * @throws IOException if file cannot be accessed or downloaded
     */
    private String resolveFilePathOrUrl(String filePathOrUrl) throws IOException {
        if (filePathOrUrl == null || filePathOrUrl.trim().isEmpty()) {
            throw new IOException("File path or URL cannot be empty");
        }
        
        // Check if the file path is a temporary file that already exists
        // This handles cases where the system has already downloaded the file to a temp location
        if (filePathOrUrl.contains("/var/folders/") || filePathOrUrl.contains("temp_") || 
            filePathOrUrl.startsWith(System.getProperty("java.io.tmpdir"))) {
            Path tempFilePath = Paths.get(filePathOrUrl);
            if (Files.exists(tempFilePath)) {
                log.info("Using existing temporary file: {}", filePathOrUrl);
                return filePathOrUrl;
            }
        }
        
        // Check if the input is a URL
        if (filePathOrUrl.toLowerCase().startsWith("http://") || filePathOrUrl.toLowerCase().startsWith("https://")) {
            log.info("Detected URL: {}", filePathOrUrl);
            try {
                // Create a URL object and open connection
                URL url = new URL(filePathOrUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(10000); // 10 seconds timeout
                connection.setReadTimeout(30000);    // 30 seconds read timeout
                
                // Set user agent to avoid potential blocking
                connection.setRequestProperty("User-Agent", "ZeoTap-Integration-Tool/1.0");
                
                // Check response code
                int responseCode = connection.getResponseCode();
                if (responseCode != HttpURLConnection.HTTP_OK) {
                    throw new IOException("Failed to get schema: HTTP error code: " + responseCode);
                }
                
                // Create a temporary file to store the downloaded content
                String tempFileName = "temp_" + UUID.randomUUID().toString() + ".csv";
                File tempFile = new File(System.getProperty("java.io.tmpdir"), tempFileName);
                
                // Download the file
                try (InputStream inputStream = connection.getInputStream()) {
                    Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
                
                log.info("Downloaded URL content to temporary file: {}", tempFile.getAbsolutePath());
                return tempFile.getAbsolutePath();
                
            } catch (MalformedURLException e) {
                throw new IOException("Invalid URL format: " + e.getMessage(), e);
            } catch (IOException e) {
                throw new IOException("Error downloading file from URL: " + e.getMessage(), e);
            }
        } else {
            // It's a local file path, verify it exists
            Path filePath = Paths.get(filePathOrUrl);
            if (!Files.exists(filePath)) {
                throw new IOException("File not found: " + filePathOrUrl);
            }
            return filePathOrUrl;
        }
    }

    /**
     * Reads the schema (column names and inferred types) from a flat file
     *
     * @param config Flat file configuration
     * @return List of column metadata
     * @throws IOException if file reading fails
     */
    public List<ColumnMetadata> readFileSchema(FlatFileConfig config) throws IOException {
        List<ColumnMetadata> columns = new ArrayList<>();
        
        // Check if config is null
        if (config == null) {
            log.error("FlatFileConfig is null");
            throw new IOException("FlatFileConfig cannot be null");
        }
        
        // Check if fileName is null
        if (config.getFileName() == null) {
            log.error("File name is null in FlatFileConfig");
            throw new IOException("File name cannot be null");
        }
        
        log.info("Attempting to resolve file path: {}", config.getFileName());
        
        // Resolve file path or URL
        String resolvedFilePath = resolveFilePathOrUrl(config.getFileName());
        
        try (Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(resolvedFilePath), Charset.forName(config.getEncoding())))) {
            // Configure CSV parser based on the delimiter
            CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withDelimiter(config.getDelimiter().charAt(0))
                    .withHeader()
                    .withSkipHeaderRecord(config.isHasHeader());
            
            try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
                // If file has header, use it for column names
                if (config.isHasHeader()) {
                    Map<String, Integer> headerMap = csvParser.getHeaderMap();
                    for (String header : headerMap.keySet()) {
                        columns.add(new ColumnMetadata(header, ""));
                    }
                    
                    // Try to infer column types from the first record
                    if (csvParser.iterator().hasNext()) {
                        CSVRecord record = csvParser.iterator().next();
                        int i = 0;
                        for (String header : headerMap.keySet()) {
                            String value = record.get(i);
                            String inferredType = inferType(value);
                            columns.get(i).setType(inferredType);
                            i++;
                        }
                    }
                } else {
                    // If no header, create generic column names
                    if (csvParser.iterator().hasNext()) {
                        CSVRecord record = csvParser.iterator().next();
                        for (int i = 0; i < record.size(); i++) {
                            String columnName = "Column_" + (i + 1);
                            String value = record.get(i);
                            String inferredType = inferType(value);
                            columns.add(new ColumnMetadata(columnName, inferredType));
                        }
                    }
                }
            }
        }
        
        return columns;
    }

    /**
     * Infers the data type of a value
     *
     * @param value String value to analyze
     * @return Inferred type name
     */
    private String inferType(String value) {
        if (value == null || value.isEmpty()) {
            return "String";
        }
        
        // Try to parse as integer
        try {
            Long.parseLong(value);
            return "Integer";
        } catch (NumberFormatException e) {
            // Not an integer
        }
        
        // Try to parse as double
        try {
            Double.parseDouble(value);
            return "Double";
        } catch (NumberFormatException e) {
            // Not a double
        }
        
        // Try to parse as date (simplified check)
        if (value.matches("\\d{4}-\\d{2}-\\d{2}") || 
            value.matches("\\d{2}/\\d{2}/\\d{4}") ||
            value.matches("\\d{2}-\\d{2}-\\d{4}")) {
            return "Date";
        }
        
        // Try to parse as boolean
        String lowerValue = value.toLowerCase();
        if (lowerValue.equals("true") || lowerValue.equals("false") ||
            lowerValue.equals("yes") || lowerValue.equals("no") ||
            lowerValue.equals("1") || lowerValue.equals("0")) {
            return "Boolean";
        }
        
        // Default to string
        return "String";
    }

    /**
     * Reads data from a flat file
     *
     * @param config  Flat file configuration
     * @param columns List of columns to read
     * @param limit   Maximum number of rows to read (for preview)
     * @return List of maps representing rows of data
     * @throws IOException if file reading fails
     */
    public List<Map<String, Object>> readData(FlatFileConfig config, List<ColumnMetadata> columns, int limit) throws IOException {
        List<Map<String, Object>> results = new ArrayList<>();
        
        // Check if config is null
        if (config == null) {
            log.error("FlatFileConfig is null");
            throw new IOException("FlatFileConfig cannot be null");
        }
        
        // Check if fileName is null
        if (config.getFileName() == null) {
            log.error("File name is null in FlatFileConfig");
            throw new IOException("File name cannot be null");
        }
        
        log.info("Attempting to resolve file path: {}", config.getFileName());
        
        // Resolve file path or URL
        String resolvedFilePath = resolveFilePathOrUrl(config.getFileName());
        
        // Get selected column names
        List<String> selectedColumnNames = new ArrayList<>();
        if (columns != null) {
            for (ColumnMetadata column : columns) {
                if (column != null && column.isSelected()) {
                    if (column.getName() != null) {
                        selectedColumnNames.add(column.getName());
                    } else {
                        log.warn("Found selected column with null name, skipping");
                    }
                }
            }
        } else {
            log.warn("Columns list is null");
        }
        
        // If no columns selected, select all columns
        if (selectedColumnNames.isEmpty()) {
            log.info("No columns selected for preview, selecting all columns");
            // If no columns are explicitly selected, select all columns
            if (columns != null && !columns.isEmpty()) {
                for (ColumnMetadata column : columns) {
                    if (column != null && column.getName() != null) {
                        selectedColumnNames.add(column.getName());
                        column.setSelected(true); // Mark as selected for preview
                    }
                }
                log.info("Auto-selected {} columns for preview", selectedColumnNames.size());
            } else {
                log.warn("No columns available for preview");
            }
            
            // Still empty? Return empty result
            if (selectedColumnNames.isEmpty()) {
                log.warn("No columns available for preview after auto-selection");
                return results;
            }
        }
        
        log.info("Reading data from file: {}", resolvedFilePath);
        log.info("Selected columns for preview: {}", selectedColumnNames);
        
        try (Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(resolvedFilePath), Charset.forName(config.getEncoding() != null ? config.getEncoding() : "UTF-8")))) {
            // Configure CSV parser based on the delimiter
            CSVFormat csvFormat = CSVFormat.DEFAULT;
            
            // Add delimiter if provided, otherwise use default comma
            if (config.getDelimiter() != null && !config.getDelimiter().isEmpty()) {
                csvFormat = csvFormat.withDelimiter(config.getDelimiter().charAt(0));
            } else {
                log.warn("No delimiter specified, using default comma");
                csvFormat = csvFormat.withDelimiter(',');
            }
            
            // Configure other CSV format options
            csvFormat = csvFormat.withHeader()
                    .withSkipHeaderRecord(config.isHasHeader())
                    .withAllowMissingColumnNames(true)
                    .withIgnoreHeaderCase(true);
            
            try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
                int count = 0;
                Map<String, Integer> headerMap = csvParser.getHeaderMap();
                log.info("CSV header map: {}", headerMap);
                
                for (CSVRecord record : csvParser) {
                    if (limit > 0 && count >= limit) {
                        break;
                    }
                    
                    Map<String, Object> row = new HashMap<>();
                    
                    // Handle the case where we're using column positions instead of names
                    if (!config.isHasHeader()) {
                        int i = 0;
                        for (String columnName : selectedColumnNames) {
                            if (i < record.size()) {
                                String value = record.get(i);
                                row.put(columnName, value);
                            } else {
                                row.put(columnName, ""); // Empty value for missing columns
                            }
                            i++;
                        }
                    } else {
                        // Using header names
                        for (String columnName : selectedColumnNames) {
                            try {
                                String value = record.get(columnName);
                                row.put(columnName, value);
                            } catch (IllegalArgumentException e) {
                                // Column name not found in header, try case-insensitive match
                                boolean found = false;
                                for (String header : headerMap.keySet()) {
                                    if (header.equalsIgnoreCase(columnName)) {
                                        String value = record.get(header);
                                        row.put(columnName, value);
                                        found = true;
                                        break;
                                    }
                                }
                                
                                if (!found) {
                                    // Still not found, add empty value
                                    row.put(columnName, "");
                                    log.warn("Column '{}' not found in CSV header", columnName);
                                }
                            }
                        }
                    }
                    
                    results.add(row);
                    count++;
                }
                
                log.info("Read {} records from file", count);
            }
        } catch (Exception e) {
            log.error("Error reading data from file: {}", e.getMessage(), e);
            log.error("File path: {}, Delimiter: {}, Has Header: {}, Encoding: {}", 
                    resolvedFilePath, 
                    config.getDelimiter(), 
                    config.isHasHeader(), 
                    config.getEncoding());
            log.error("Selected columns: {}", selectedColumnNames);
            throw new IOException("Error reading data from file: " + e.getMessage(), e);
        }
        
        return results;
    }

    /**
     * Transfers data from a flat file to a target handler
     *
     * @param config  Flat file configuration
     * @param columns List of columns to transfer
     * @param handler DataHandler to process each row
     * @return Number of records processed
     * @throws IOException  if file reading fails
     * @throws SQLException if data handling fails
     */
    public int transferDataFromFlatFile(FlatFileConfig config, List<ColumnMetadata> columns, 
                                      ClickHouseService.DataHandler handler) throws IOException, SQLException {
        int recordCount = 0;
        
        // Check if config is null
        if (config == null) {
            log.error("FlatFileConfig is null");
            throw new IOException("FlatFileConfig cannot be null");
        }
        
        // Check if fileName is null
        if (config.getFileName() == null) {
            log.error("File name is null in FlatFileConfig");
            throw new IOException("File name cannot be null");
        }
        
        log.info("Attempting to resolve file path: {}", config.getFileName());
        
        // Resolve file path or URL
        String resolvedFilePath = resolveFilePathOrUrl(config.getFileName());
        
        // Get selected column names
        List<String> selectedColumnNames = new ArrayList<>();
        if (columns != null) {
            for (ColumnMetadata column : columns) {
                if (column != null && column.isSelected()) {
                    if (column.getName() != null) {
                        selectedColumnNames.add(column.getName());
                    } else {
                        log.warn("Found selected column with null name, skipping");
                    }
                }
            }
        } else {
            log.warn("Columns list is null");
        }
        
        // If no columns selected, return 0
        if (selectedColumnNames.isEmpty()) {
            log.warn("No columns selected for transfer");
            return 0;
        }
        
        log.info("Transferring data from file: {}", resolvedFilePath);
        log.info("Selected columns for transfer: {}", selectedColumnNames);
        
        try (Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(resolvedFilePath), Charset.forName(config.getEncoding() != null ? config.getEncoding() : "UTF-8")))) {
            // Configure CSV parser based on the delimiter
            CSVFormat csvFormat = CSVFormat.DEFAULT;
            
            // Add delimiter if provided, otherwise use default comma
            if (config.getDelimiter() != null && !config.getDelimiter().isEmpty()) {
                csvFormat = csvFormat.withDelimiter(config.getDelimiter().charAt(0));
            } else {
                log.warn("No delimiter specified, using default comma");
                csvFormat = csvFormat.withDelimiter(',');
            }
            
            // Configure other CSV format options
            csvFormat = csvFormat.withHeader()
                    .withSkipHeaderRecord(config.isHasHeader())
                    .withAllowMissingColumnNames(true)
                    .withIgnoreHeaderCase(true);
            
            try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
                Map<String, Integer> headerMap = csvParser.getHeaderMap();
                log.info("CSV header map: {}", headerMap);
                
                for (CSVRecord record : csvParser) {
                    Map<String, Object> row = new HashMap<>();
                    
                    // Handle the case where we're using column positions instead of names
                    if (!config.isHasHeader()) {
                        int i = 0;
                        for (String columnName : selectedColumnNames) {
                            if (i < record.size()) {
                                String value = record.get(i);
                                row.put(columnName, value);
                            } else {
                                row.put(columnName, ""); // Empty value for missing columns
                            }
                            i++;
                        }
                    } else {
                        // Using header names
                        for (String columnName : selectedColumnNames) {
                            try {
                                String value = record.get(columnName);
                                row.put(columnName, value);
                            } catch (IllegalArgumentException e) {
                                // Column name not found in header, try case-insensitive match
                                boolean found = false;
                                for (String header : headerMap.keySet()) {
                                    if (header.equalsIgnoreCase(columnName)) {
                                        String value = record.get(header);
                                        row.put(columnName, value);
                                        found = true;
                                        break;
                                    }
                                }
                                
                                if (!found) {
                                    // Still not found, add empty value
                                    row.put(columnName, "");
                                    log.warn("Column '{}' not found in CSV header", columnName);
                                }
                            }
                        }
                    }
                    
                    handler.processRow(row);
                    recordCount++;
                    
                    // Log progress every 1000 records
                    if (recordCount % 1000 == 0) {
                        log.info("Processed {} records", recordCount);
                    }
                }
                
                log.info("Transferred {} records from file", recordCount);
            }
        } catch (Exception e) {
            log.error("Error transferring data from file: {}", e.getMessage(), e);
            throw new IOException("Error transferring data from file: " + e.getMessage(), e);
        }
        
        handler.complete();
        return recordCount;
    }

    /**
     * Writes data to a flat file
     *
     * @param config  Flat file configuration
     * @param columns List of columns to write
     * @param data    List of data rows
     * @return Number of records written
     * @throws IOException if file writing fails
     */
    public int writeDataToFlatFile(FlatFileConfig config, List<ColumnMetadata> columns, 
                                 List<Map<String, Object>> data) throws IOException {
        if (data.isEmpty()) {
            return 0;
        }
        
        // Get selected column names
        List<String> selectedColumnNames = new ArrayList<>();
        if (columns != null) {
            for (ColumnMetadata column : columns) {
                if (column != null && column.isSelected()) {
                    if (column.getName() != null) {
                        selectedColumnNames.add(column.getName());
                    } else {
                        log.warn("Found selected column with null name, skipping");
                    }
                }
            }
        } else {
            log.warn("Columns list is null");
        }
        
        // If no columns selected, return 0
        if (selectedColumnNames.isEmpty()) {
            return 0;
        }
        
        // For target files, we always use the local file path directly
        Path filePath = Paths.get(config.getFileName());
        
        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(config.getFileName()), Charset.forName(config.getEncoding())))) {
            // Configure CSV printer based on the delimiter
            CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withDelimiter(config.getDelimiter().charAt(0))
                    .withHeader(selectedColumnNames.toArray(new String[0]));
            
            try (CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {
                for (Map<String, Object> row : data) {
                    List<Object> recordValues = new ArrayList<>();
                    for (String columnName : selectedColumnNames) {
                        recordValues.add(row.get(columnName));
                    }
                    
                    csvPrinter.printRecord(recordValues);
                }
            }
        }
        
        return data.size();
    }

    /**
     * Creates a DataHandler for writing to a flat file
     *
     * @param config  Flat file configuration
     * @param columns List of columns to write
     * @return DataHandler for writing to the flat file
     */
    public ClickHouseService.DataHandler createFlatFileDataHandler(FlatFileConfig config, List<ColumnMetadata> columns) {
        // Get selected column names
        List<String> selectedColumnNames = new ArrayList<>();
        if (columns != null) {
            for (ColumnMetadata column : columns) {
                if (column != null && column.isSelected()) {
                    if (column.getName() != null) {
                        selectedColumnNames.add(column.getName());
                    } else {
                        log.warn("Found selected column with null name, skipping");
                    }
                }
            }
        } else {
            log.warn("Columns list is null");
        }
        
        return new ClickHouseService.DataHandler() {
            private CSVPrinter csvPrinter;
            private Writer writer;
            private int recordCount = 0;
            
            @Override
            public void processRow(Map<String, Object> row) throws SQLException {
                try {
                    if (csvPrinter == null) {
                        // Initialize CSV printer on first row
                        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(config.getFileName()), Charset.forName(config.getEncoding())));
                        CSVFormat csvFormat = CSVFormat.DEFAULT
                                .withDelimiter(config.getDelimiter().charAt(0))
                                .withHeader(selectedColumnNames.toArray(new String[0]));
                        csvPrinter = new CSVPrinter(writer, csvFormat);
                    }
                    
                    List<Object> recordValues = new ArrayList<>();
                    for (String columnName : selectedColumnNames) {
                        recordValues.add(row.get(columnName));
                    }
                    
                    csvPrinter.printRecord(recordValues);
                    recordCount++;
                    
                    // Log progress every 1000 records
                    if (recordCount % 1000 == 0) {
                        log.info("Written {} records to flat file", recordCount);
                    }
                } catch (IOException e) {
                    throw new SQLException("Error writing to flat file: " + e.getMessage(), e);
                }
            }
            
            @Override
            public void complete() throws SQLException {
                try {
                    if (csvPrinter != null) {
                        csvPrinter.close();
                    }
                    if (writer != null) {
                        writer.close();
                    }
                    log.info("Completed writing {} records to flat file", recordCount);
                } catch (IOException e) {
                    throw new SQLException("Error closing flat file: " + e.getMessage(), e);
                }
            }
        };
    }
}