/**
 * Copyright 2012 Twitter, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import view.Main;

public class ConvertUtils {

    private static final Log LOG = Log.getLog(ConvertUtils.class);

    public static String CSV_DELIMITER = ",";
    public static boolean allowOverwrite = false;
    private static int lastColumnCount = 0;
    public static int lastProcessedLineNumber = 0;

    public static void setCsvDelimiter(String d) {
        CSV_DELIMITER = d;
    }

    public static void setAllowOverwrite(boolean a) {
        allowOverwrite = a;
    }

    private static String readFile(String path) throws IOException {
        return readFile(path, -1);
    }

    private static String readFile(String path, int numberOfLines) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        StringBuilder stringBuilder = new StringBuilder();

        try {
            String line = null;
            String ls = "\n";//System.getProperty("line.separator");

            while (numberOfLines != 0 && (line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
                numberOfLines--;
            }
        } finally {
            Utils.closeQuietly(reader);
        }

        return stringBuilder.toString();
    }

    public static String getSchema(File csvFile) throws IOException {
        String fileName = csvFile.getName().substring(
                0, csvFile.getName().length() - ".csv".length()) + ".schema";
        File schemaFile = new File(csvFile.getParentFile(), fileName);
        return readFile(schemaFile.getAbsolutePath());
    }

    public static String generateSchemaFromFile(File csvFile, int extraColumns) throws IOException {
        String headerLine = readFile(csvFile.getAbsolutePath(), 1);
        List<String> columnList = convertCsvRowToList(headerLine);
        columnList = new ArrayList<String>(columnList);
        StringBuilder sb = new StringBuilder();
        sb.append("message m {\n");
        int columnCount = 0;
        if(extraColumns > 0) {
            for(int i=0; i< extraColumns;i++) {
                columnList.add("ExtraHeader#" + (i+1));
            }
        }
        for(String column: columnList) {
            if(column == null || column.isEmpty()) {
                column = "Header#" + columnCount;
            }
            columnCount++;
            sb.append("optional binary ").append(column.replace(" ", "_").replace("\n","")).append(";\n");
        }
        sb.append("}");
        return sb.toString();
    }

    public static List<String> convertCsvRowToList(String row) {
        List<String> columns = new ArrayList<String>();
        StringBuilder lastColumn = new StringBuilder();
        boolean canEnd = true;
        for(int i=0; i<row.length();i++) {
            char c = row.charAt(i);
            if(c == CSV_DELIMITER.charAt(0) && canEnd) {
                columns.add(lastColumn.toString().trim());
                lastColumn = new StringBuilder();
                continue;
            }
            if(c == '"') {
                if(canEnd) {
                    canEnd = false;
                } else {
                    if((i+1 < row.length()) && row.charAt(i+1) == '"') {
                        lastColumn.append("\"");
                        i++;
                    } else {
                        canEnd = true;
                    }
                }
            } else {
                lastColumn.append(c);
            }
        }
        columns.add(lastColumn.toString().trim());
        return columns;
    }

    public static int convertCsvToParquet(File csvFile, File outputParquetFile, boolean generateSchema, int extraColumns) throws IOException {
        return convertCsvToParquet(csvFile, outputParquetFile, false, generateSchema, extraColumns);
    }

    public static int convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary, boolean generateSchema, int extraColumns) throws IOException {
        LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
        String rawSchema = generateSchema ? generateSchemaFromFile(csvFile, extraColumns): getSchema(csvFile);
        if (outputParquetFile.exists()) {
            if(!allowOverwrite) {
                throw new IOException("Output file " + outputParquetFile.getAbsolutePath() +
                        " already exists");
            } else {
                outputParquetFile.delete();
            }
        }
        lastColumnCount = rawSchema.split(";").length - 1;
        Path path = new Path(outputParquetFile.toURI());

        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
        CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);


        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        int lineNumber = -1;
        lastProcessedLineNumber = -1;
        int maxRowSize = lastColumnCount;
        try {
            while ((line = br.readLine()) != null) {
                lineNumber++;
                Main.message.setText("Processing line: " + lineNumber);
                lastProcessedLineNumber = lineNumber;
                if(generateSchema && lineNumber == 0) {
                    continue;
                }
                List<String> fieldList = convertCsvRowToList(line);
                fieldList = new ArrayList<String>(fieldList);
                if(fieldList.size() < lastColumnCount) {
                    for(int i=fieldList.size(); i< lastColumnCount; i++) {
                        fieldList.add("");
                    }
                }
                if(fieldList.size() > lastColumnCount) {
                    maxRowSize = Math.max(maxRowSize, fieldList.size());
                    for(int i= fieldList.size(); i > lastColumnCount && i>0; i--) {
                        fieldList.remove(i - 1);
                    }
                }
                for(int i=0; i<fieldList.size(); i++) {
                    String data = fieldList.get(i);
                    if(data == null || data.isEmpty()) {
                        fieldList.set(i, "");
                    }
//                    if(data.contains("\"")) {
//                        fieldList.set(i, data.replaceAll(Pattern.quote("\""), ""));
//                    }
                }
                writer.write(fieldList);
            }

            writer.close();
            String baseFileName = outputParquetFile.getName();
            File crcFile = new File(outputParquetFile.getAbsolutePath().substring(0, outputParquetFile.getAbsolutePath().length() - baseFileName.length()) + "." + baseFileName + ".crc");
            if (crcFile.exists()) {
                crcFile.delete();
            }
        } finally {
            LOG.info("Number of lines: " + lineNumber);
            Utils.closeQuietly(br);
        }
        return maxRowSize - lastColumnCount;
    }

    public static void convertParquetToCSV(File parquetFile, File csvOutputFile) throws IOException {
        Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"),
                "parquet file should have .parquet extension");
        Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"),
                "csv file should have .csv extension");
        Preconditions.checkArgument(!csvOutputFile.exists(),
                "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

        LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());


        Path parquetFilePath = new Path(parquetFile.toURI());

        Configuration configuration = new Configuration(true);

        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
        MessageType schema = readFooter.getFileMetaData().getSchema();

        readSupport.init(configuration, null, schema);
        BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
        ParquetReader<Group> reader = new ParquetReader<Group>(parquetFilePath, readSupport);
        try {
            Group g = null;
            while ((g = reader.read()) != null) {
                writeGroup(w, g, schema);
            }
            reader.close();
        } finally {
            Utils.closeQuietly(w);
        }
    }

    private static void writeGroup(BufferedWriter w, Group g, MessageType schema)
            throws IOException {
        for (int j = 0; j < schema.getFieldCount(); j++) {
            if (j > 0) {
                w.write(CSV_DELIMITER);
            }
            String valueToString = g.getValueToString(j, 0);
            w.write(valueToString);
        }
        w.write('\n');
    }

    @Deprecated
    public static void convertParquetToCSVEx(File parquetFile, File csvOutputFile) throws IOException {
        Preconditions.checkArgument(parquetFile.getName().endsWith(".parquet"),
                "parquet file should have .parquet extension");
        Preconditions.checkArgument(csvOutputFile.getName().endsWith(".csv"),
                "csv file should have .csv extension");
        Preconditions.checkArgument(!csvOutputFile.exists(),
                "Output file " + csvOutputFile.getAbsolutePath() + " already exists");

        LOG.info("Converting " + parquetFile.getName() + " to " + csvOutputFile.getName());

        Path parquetFilePath = new Path(parquetFile.toURI());

        Configuration configuration = new Configuration(true);

        // TODO Following can be changed by using ParquetReader instead of ParquetFileReader
        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, parquetFilePath);
        MessageType schema = readFooter.getFileMetaData().getSchema();
        ParquetFileReader parquetFileReader = new ParquetFileReader(
                configuration, parquetFilePath, readFooter.getBlocks(), schema.getColumns());
        BufferedWriter w = new BufferedWriter(new FileWriter(csvOutputFile));
        PageReadStore pages = null;
        try {
            while (null != (pages = parquetFileReader.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                LOG.info("Number of rows: " + rows);

                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final Group g = recordReader.read();
                    writeGroup(w, g, schema);
                }
            }
        } finally {
            Utils.closeQuietly(parquetFileReader);
            Utils.closeQuietly(w);
        }
    }

}
