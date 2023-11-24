package com.khomchenko;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Usage: java -jar ВашФайл.jar <jdbcUrl> <username> <password> <csvFilePath> <tableName>");
            return;
        }
        String jdbcUrl = args[0];
        String username = args[1];
        String password = args[2];
        String csvFilePath = args[3];
        String tableName = args[4];

        Connection connection = null;
        BufferedReader bufferedReader = null;
        CSVReader reader = null;

        try{
             connection = DriverManager.getConnection(jdbcUrl, username, password);
             bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFilePath), "cp1251"));
             reader = new CSVReaderBuilder(bufferedReader)
                     .withCSVParser(new CSVParserBuilder()
                             .withQuoteChar('"')
                             .withSeparator(';')
                             .withEscapeChar('\0')
                             .build())
                     .build();

            String[] header = reader.readNext();
            System.out.println(Arrays.toString(header));
            createTable(connection, tableName, header);
            insertData(connection, reader, tableName, header);

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }

                if (bufferedReader != null) {
                    bufferedReader.close();
                }

                if (reader != null) {
                    reader.close();
                }
            } catch (IOException | SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void createTable(Connection connection, String fullTableName, String[] columnNames) throws Exception {
        String[] parts = fullTableName.split("\\.");
        String schema = null;
        String tableName = fullTableName;

        if (parts.length == 2) {
            schema = parts[0];
            tableName = parts[1];
        }

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getTables(null, schema, tableName, null);
        if (resultSet.next()) {
            System.out.println("Table " + fullTableName + " already exists. Skipping creation.");
            return;
        }

        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + fullTableName + " (");
        for (int i = 0; i < columnNames.length; i++) {
            createTableQuery.append("\"").append(columnNames[i]).append("\" varchar(5000)");
            if (i < columnNames.length - 1) {
                createTableQuery.append(", ");
            }
        }
        createTableQuery.append(")");

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(createTableQuery.toString());
        }
    }


    private static void insertData(Connection connection, CSVReader reader, String tableName, String[] columnNames) throws Exception {
        StringBuilder questionMarks = new StringBuilder("?");
        for (int i = 1; i < columnNames.length; i++) {
            questionMarks.append(", ?");
        }
        String insertQuery = "INSERT INTO " + tableName + " VALUES (" + questionMarks + ")";

        try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
            int batchSize = 5000;
            int count = 0;
            String[] line;
            while ((line = reader.readNext()) != null) {
                for (int i = 0; i < line.length; i++) {
                    preparedStatement.setString(i + 1, line[i]);
                }
                preparedStatement.addBatch();

                if (++count % batchSize == 0) {
                    preparedStatement.executeBatch();
                }
            }
            preparedStatement.executeBatch();
        }
    }
}