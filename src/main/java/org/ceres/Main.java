package org.ceres;

import org.apache.logging.log4j.simple.SimpleLogger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class Main {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/sunlyte_centenary";
        String user = "postgres";
        String password = "toor";

        Logger logger = Logger.getLogger(Main.class.getName());

        // Connection and Statement objects
        Connection conn = null;
        Statement tableStmt = null;
        Statement columnStmt = null;
        ResultSet rsTables = null;
        ResultSet rsColumns = null;

        XSSFWorkbook workbook = new XSSFWorkbook();
        XSSFSheet sheet = workbook.createSheet("Dictionary_Sheet" + UUID.randomUUID());

        try {
            conn = DriverManager.getConnection(url, user, password);
            logger.info("Connection to PostgreSQL has been established.");

            // Creating a Statement object
            tableStmt = conn.createStatement();

            // Query to list all tables
            String sqlTables = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'";
            rsTables = tableStmt.executeQuery(sqlTables);

            AtomicInteger rowNum = new AtomicInteger();


            while (rsTables.next()) {
                String tableName = rsTables.getString("table_name");

                // Query to get column details
                String sqlColumns = String.format(
                        """
                                SELECT
                                    cols.column_name,
                                    cols.data_type,
                                    cols.is_nullable,
                                    cols.column_default,
                                    cols.character_maximum_length,
                                    cols.numeric_precision,
                                    cols.numeric_scale,
                                    pgd.description AS column_comment,
                                    CASE
                                        WHEN kcu.column_name IS NOT NULL THEN 'YES'
                                        ELSE 'NO'
                                    END AS is_primary_key,
                                    tc.constraint_name AS is_foreign_key,
                                    ccu.table_name AS foreign_table_name,
                                    ccu.column_name AS foreign_column_name
                                FROM
                                    information_schema.columns cols
                                LEFT JOIN
                                    pg_catalog.pg_statio_all_tables as st
                                    ON st.schemaname = cols.table_schema
                                    AND st.relname = cols.table_name
                                LEFT JOIN
                                    pg_catalog.pg_description pgd
                                    ON pgd.objoid = st.relid
                                    AND pgd.objsubid = cols.ordinal_position
                                LEFT JOIN
                                    information_schema.table_constraints tc
                                    ON tc.table_name = cols.table_name
                                    AND tc.constraint_type = 'FOREIGN KEY'
                                    AND tc.table_schema = cols.table_schema
                                LEFT JOIN
                                    information_schema.constraint_column_usage ccu
                                    ON ccu.constraint_name = tc.constraint_name
                                    AND ccu.table_schema = tc.table_schema
                                LEFT JOIN
                                    information_schema.key_column_usage kcu
                                    ON kcu.table_name = cols.table_name
                                    AND kcu.table_schema = cols.table_schema
                                    AND kcu.constraint_name = tc.constraint_name
                                    AND kcu.column_name = cols.column_name
                                WHERE
                                    cols.table_name = '%s';
                                """, tableName);
                columnStmt = conn.createStatement();
                rsColumns = columnStmt.executeQuery(sqlColumns);

                Row tableNameRow = sheet.createRow(rowNum.getAndIncrement());
                Cell tableNameCell = tableNameRow.createCell(0);
                tableNameCell.setCellValue(tableName.toUpperCase().replace("_", " "));
                createHeaderRows(sheet, rowNum);

                while (rsColumns.next()) {
                    Row row = sheet.createRow(rowNum.getAndIncrement());

                    Cell columnNameCell = row.createCell(0);
                    columnNameCell.setCellValue(rsColumns.getString("column_name"));

                    Cell dataTypeCell = row.createCell(1);
                    dataTypeCell.setCellValue(rsColumns.getString("data_type"));

                    Cell semanticRulesCell = row.createCell(2);
                    semanticRulesCell.setCellValue("Semantic rules here"); // Placeholder for semantic rules

                    Cell isPrimaryKeyCell = row.createCell(3);
                    isPrimaryKeyCell.setCellValue(rsColumns.getString("is_primary_key"));

                    Cell isForeignKeyCell = row.createCell(4);
                    isForeignKeyCell.setCellValue(rsColumns.getString("is_foreign_key") != null ? "YES" : "NO");

                    Cell foreignTableCell = row.createCell(5);
                    foreignTableCell.setCellValue(rsColumns.getString("foreign_table_name"));

                    Cell foreignColumnCell = row.createCell(6);
                    foreignColumnCell.setCellValue(rsColumns.getString("foreign_column_name"));

                    Cell nullableCell = row.createCell(7);
                    nullableCell.setCellValue(rsColumns.getString("is_nullable"));

                    Cell sampleValueCell = row.createCell(8);
                    sampleValueCell.setCellValue("Sample value here"); // Placeholder for sample values

                    Cell columnDescriptionCell = row.createCell(9);
                    columnDescriptionCell.setCellValue(rsColumns.getString("column_comment"));
                }

                // Empty row to separate tables
                rowNum.getAndIncrement();
            }

            try (FileOutputStream out = new FileOutputStream("DataDictionary" + UUID.randomUUID() + ".xlsx")) {
                workbook.write(out);
            }
            logger.info("Data Dictionary Successfully Generated.");

        } catch (SQLException e) {
            logger.info(e.getMessage());
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            closeResultSet(rsTables);
            closeResultSet(rsColumns);
            closeStatement(tableStmt);
            closeStatement(columnStmt);
            closeConnection(conn);
        }
    }

    private static void createHeaderRows(XSSFSheet sheet, AtomicInteger rowNum) {
        Row headerRow = sheet.createRow(rowNum.getAndIncrement());

        headerRow.createCell(0).setCellValue("Column Name");
        headerRow.createCell(1).setCellValue("Data Type");
        headerRow.createCell(2).setCellValue("Semantic rules");
        headerRow.createCell(3).setCellValue("IS Primary Key");
        headerRow.createCell(4).setCellValue("Is foreign key");
        headerRow.createCell(5).setCellValue("Reference table for foreign key column");
        headerRow.createCell(6).setCellValue("Referenced table column");
        headerRow.createCell(7).setCellValue("Nullable");
        headerRow.createCell(8).setCellValue("Sample Value");
        headerRow.createCell(9).setCellValue("Description of the column.");
    }

    private static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    private static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    private static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
}
