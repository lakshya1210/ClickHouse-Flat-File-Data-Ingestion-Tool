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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for handling Flat File operations
 */
@Service
@Slf4j
public class FlatFileService {

    /**
     * Reads the schema (column names and inferred types) from a flat file
     *
     * @param config Flat file configuration
     * @return List of column metadata
     * @throws IOException if file reading fails
     */
    public List<ColumnMetadata> readFileSchema(FlatFileConfig config) throws IOException {
        List<ColumnMetadata> columns = new ArrayList<>();
        Path filePath = Paths.get(config.getFileName());
        
        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + config.getFileName());
        }
        
        try (Reader reader = new BufferedReader(new FileReader(config.getFileName(), Charset.forName(config.getEncoding())))) {
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
        Path filePath = Paths.get(config.getFileName());
        
        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + config.getFileName());
        }
        
        // Get selected column names
        List<String> selectedColumnNames = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (column.isSelected()) {
                selectedColumnNames.add(column.getName());
            }
        }
        
        // If no columns selected, return empty result
        if (selectedColumnNames.isEmpty()) {
            return results;
        }
        
        try (Reader reader = new BufferedReader(new FileReader(config.getFileName(), Charset.forName(config.getEncoding())))) {
            // Configure CSV parser based on the delimiter
            CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withDelimiter(config.getDelimiter().charAt(0))
                    .withHeader()
                    .withSkipHeaderRecord(config.isHasHeader());
            
            try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
                int count = 0;
                for (CSVRecord record : csvParser) {
                    if (limit > 0 && count >= limit) {
                        break;
                    }
                    
                    Map<String, Object> row = new HashMap<>();
                    for (String columnName : selectedColumnNames) {
                        String value = record.get(columnName);
                        row.put(columnName, value);
                    }
                    
                    results.add(row);
                    count++;
                }
            }
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
        Path filePath = Paths.get(config.getFileName());
        
        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + config.getFileName());
        }
        
        // Get selected column names
        List<String> selectedColumnNames = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (column.isSelected()) {
                selectedColumnNames.add(column.getName());
            }
        }
        
        // If no columns selected, return 0
        if (selectedColumnNames.isEmpty()) {
            return 0;
        }
        
        try (Reader reader = new BufferedReader(new FileReader(config.getFileName(), Charset.forName(config.getEncoding())))) {
            // Configure CSV parser based on the delimiter
            CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withDelimiter(config.getDelimiter().charAt(0))
                    .withHeader()
                    .withSkipHeaderRecord(config.isHasHeader());
            
            try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
                for (CSVRecord record : csvParser) {
                    Map<String, Object> row = new HashMap<>();
                    for (String columnName : selectedColumnNames) {
                        String value = record.get(columnName);
                        row.put(columnName, value);
                    }
                    
                    handler.processRow(row);
                    recordCount++;
                    
                    // Log progress every 1000 records
                    if (recordCount % 1000 == 0) {
                        log.info("Processed {} records", recordCount);
                    }
                }
            }
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
        for (ColumnMetadata column : columns) {
            if (column.isSelected()) {
                selectedColumnNames.add(column.getName());
            }
        }
        
        // If no columns selected, return 0
        if (selectedColumnNames.isEmpty()) {
            return 0;
        }
        
        Path filePath = Paths.get(config.getFileName());
        
        try (Writer writer = new BufferedWriter(new FileWriter(config.getFileName(), Charset.forName(config.getEncoding())))) {
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
        for (ColumnMetadata column : columns) {
            if (column.isSelected()) {
                selectedColumnNames.add(column.getName());
            }
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
                        writer = new BufferedWriter(new FileWriter(config.getFileName(), Charset.forName(config.getEncoding())));
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