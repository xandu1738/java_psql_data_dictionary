package org.ceres;

import java.sql.*;

public class Main {


    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/hooker";
        String user = "postgres";
        String password = "toor";

        // Connection and Statement objects
        Connection conn = null;
        Statement stmt = null;
        ResultSet rsTables = null;
        ResultSet rsColumns = null;
        ResultSet rsPrimaryKeys = null;
        ResultSet rsForeignKeys = null;
        ResultSet rsComments = null;

        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connection to PostgreSQL has been established.");

            // Creating a Statement object
            stmt = conn.createStatement();

            // Query to list all tables
            String sqlTables = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'";
            Statement stmt2 = conn.createStatement();
            rsTables = stmt2.executeQuery(sqlTables);

            while (rsTables.next()) {
                String tableName = rsTables.getString("table_name");
                System.out.println("Table: " + tableName);

                // Query to get column details
                String sqlColumns = String.format(
                        "SELECT column_name, data_type, is_nullable, column_default, " +
                        "character_maximum_length, numeric_precision, numeric_scale " +
                        "FROM information_schema.columns WHERE table_name = '%s'", tableName);
                Statement stmt3 = conn.createStatement();
                rsColumns = stmt3.executeQuery(sqlColumns);

                while (rsColumns.next()) {
                    String columnName = rsColumns.getString("column_name");
                    String dataType = rsColumns.getString("data_type");
                    String isNullable = rsColumns.getString("is_nullable");
                    String columnDefault = rsColumns.getString("column_default");
                    String maxLength = rsColumns.getString("character_maximum_length");
                    String numericPrecision = rsColumns.getString("numeric_precision");
                    String numericScale = rsColumns.getString("numeric_scale");

                    System.out.println("  Column: " + columnName);
                    System.out.println("    Data Type: " + dataType);
                    System.out.println("    Nullable: " + isNullable);
                    System.out.println("    Default: " + columnDefault);
                    System.out.println("    Max Length: " + maxLength);
                    System.out.println("    Numeric Precision: " + numericPrecision);
                    System.out.println("    Numeric Scale: " + numericScale);
                }

                // Query to get primary keys
                String sqlPrimaryKeys = String.format(
                        "SELECT a.attname AS column_name " +
                        "FROM pg_index i " +
                        "JOIN pg_attribute a ON a.attnum = ANY(i.indkey) " +
                        "WHERE i.indrelid = '%s'::regclass AND i.indisprimary", tableName);
                Statement stmt4 = conn.createStatement();
                rsPrimaryKeys = stmt4.executeQuery(sqlPrimaryKeys);

                System.out.print("  Primary Keys: ");
                while (rsPrimaryKeys.next()) {
                    String primaryKey = rsPrimaryKeys.getString("column_name");
                    System.out.print(primaryKey + " ");
                }
                System.out.println();

                // Query to get foreign keys
                String sqlForeignKeys = String.format(
                        "SELECT kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name " +
                        "FROM information_schema.key_column_usage kcu " +
                        "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = kcu.constraint_name " +
                        "WHERE kcu.table_name = '%s' AND kcu.constraint_name IN (" +
                        "SELECT constraint_name FROM information_schema.table_constraints " +
                        "WHERE constraint_type = 'FOREIGN KEY' AND table_name = '%s')", tableName, tableName);
                Statement stmt5 = conn.createStatement();
                rsForeignKeys = stmt5.executeQuery(sqlForeignKeys);

                System.out.print("  Foreign Keys: ");
                while (rsForeignKeys.next()) {
                    String columnName = rsForeignKeys.getString("column_name");
                    String foreignTableName = rsForeignKeys.getString("foreign_table_name");
                    String foreignColumnName = rsForeignKeys.getString("foreign_column_name");
                    System.out.println("    Column: " + columnName);
                    System.out.println("      References: " + foreignTableName + "(" + foreignColumnName + ")");
                }

                // Query to get column comments
                String sqlComments = String.format(
                        "SELECT column_name, col_description('%s'::regclass, ordinal_position) AS column_comment " +
                        "FROM information_schema.columns WHERE table_name = '%s'", tableName, tableName);
                Statement stmt6 = conn.createStatement();
                rsComments = stmt6.executeQuery(sqlComments);

                while (rsComments.next()) {
                    String columnName = rsComments.getString("column_name");
                    String comment = rsComments.getString("column_comment");
                    System.out.println("  Column: " + columnName + " - Comment: " + comment);
                }
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            // Close ResultSets
            if (rsTables != null) {
                try {
                    rsTables.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            if (rsColumns != null) {
                try {
                    rsColumns.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            if (rsPrimaryKeys != null) {
                try {
                    rsPrimaryKeys.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            if (rsForeignKeys != null) {
                try {
                    rsForeignKeys.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            if (rsComments != null) {
                try {
                    rsComments.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            // Close Statement
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
            // Close Connection
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }
    }
}
