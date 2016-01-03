/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.catais.av.av_avdpool_next;

import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.base.Ili2dbException;
import ch.ehi.ili2db.gui.Config;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.io.FilenameUtils;
import org.catais.av.av_avdpool_next.ili2db.Ili2dbConfig;

/**
 *
 * @author stefan
 */
public class PostgresqlDatabase {

    static final Logger logger = LogManager.getLogger(PostgresqlDatabase.class.getName());

    // Shared database parameters (tmp=import && final=target schema)
    String dbhost = null;
    String dbport = null;
    String dbdatabase = null;
    String dbusr = null;
    String dbpwd = null;
    String modeldir = null;
    String loglevel = null;
    String dburl = null;

    // Temporary (=import) database parameters
    String dbschemaTmp = null;
    String modelsTmp = null;

    // Final (=target) database parameters
    String dbschema = null;
    String models = null;

    // Read only database user
    String dbusrro = null;
    String dbpwdro = null;

    HashMap<String, String> params = null;

    public PostgresqlDatabase(HashMap<String, String> params) {
        this.params = params;

        dbhost = params.get("dbhost");
        dbport = params.get("dbport");
        dbdatabase = params.get("dbdatabase");
        dbusr = params.get("dbusr");
        dbpwd = params.get("dbpwd");
        loglevel = params.get("loglevel");
        dburl = "jdbc:postgresql://" + dbhost + ":" + dbport + "/" + dbdatabase + "?user=" + dbusr + "&password=" + dbpwd;

        dbschemaTmp = params.get("dbschemaTmp");
        modelsTmp = params.get("modelsTmp");

        dbschema = params.get("dbschema");
        models = params.get("models");

        dbusrro = params.get("dbusrro");
        dbpwdro = params.get("dbusrro");
    }

    // initSchema() is not not transactional. It's 'only' an init-
    // method this shouldn't be a problem since it will be performed rarely.
    // TODO: Proper exception handling (for the whole process):
    // There will be no rollback if one of the three tasks fail.
    // So we can simple abort the whole process if there is an exception 
    // thrown.
    // Yes?
    public void initSchema() throws Ili2dbException, ClassNotFoundException, SQLException {
        // Create final schema
        Ili2dbConfig ili2dbConfig = new Ili2dbConfig(params);
        Config config = ili2dbConfig.getConfig(this.dbschema, this.models);
        // We don't need a sequenced based t_id in the final tables.
        // The t_id from the temporary tables can and will be used.
        // These temporary t_id are sequenced based.
        // TODO: Someday the sequence reaches its maximum. What will happen?
        config.setIdGenerator(ch.ehi.ili2db.base.TableBasedIdGen.class.getName());
        Ili2db.runSchemaImport(config, "");

        // Grant schema and tables to public user.
        grantSchemaTables();

        // Create temporary schema
        config = ili2dbConfig.getConfig(this.dbschemaTmp, this.modelsTmp);
        Ili2db.runSchemaImport(config, "");
    }

    public ArrayList importItf() {
        // Now and here we set the date of delivery which will be used for all
        // files we import.
        Date deliveryDate = new Date();

        // Some file lists
        ArrayList<String> itfFileNames = new ArrayList<>();
        ArrayList<String> failedImports = new ArrayList<>();

        // Get an ili2db configuration.
        // Delete all data (in the temporary) schema with .setDeleteMode()
        // (--deleteData).
        // .setConfigReadFromDb() is necessary since the schema/tables exists.
        Ili2dbConfig ili2dbConfig = new Ili2dbConfig(params);
        Config config = ili2dbConfig.getConfig(this.dbschemaTmp, this.modelsTmp);
        config.setDeleteMode(Config.DELETE_DATA);
        config.setConfigReadFromDb(true);

        // Download and import directory.
        // The directory where the itf files can be found that we want
        // to import.
        String importDir = params.get("ftpDownloadDir");
        logger.debug("importDir-String: " + importDir);

        Path importDirPath = Paths.get(importDir);
        logger.debug("importDir-Path: " + importDirPath);

        // Loop through the directory with the zip files, unzip and import
        // them.
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(importDirPath, "*.zip")) {
            for (Path entry : stream) {
                try {
                    logger.debug("Importing file: " + entry);
                    // Get fosnr (=gem_bfs) and lot (=los) from file name.
                    // Exception will be thrown if it is not named correctly.
                    String fosnrStr = entry.getFileName().toString().substring(0, 4);
                    String lotStr = entry.getFileName().toString().substring(4, 6);

                    int fosnr = Integer.parseInt(fosnrStr);
                    int lot = Integer.parseInt(lotStr);

                    // Check if it is LV95.
                    String lv95 = entry.getFileName().toString().substring(7, 11);
                    if (!lv95.equalsIgnoreCase("lv95")) {
                        logger.error("No lv95 file: " + entry.getFileName().toString());
                        logger.error("Will continue with next file.");
                        continue;
                    }

                    // We need to unzip the files first.
                    File itfFile = unzipItf(entry.toAbsolutePath());

                    if (itfFile != null) {
                        itfFileNames.add(itfFile.getAbsolutePath());
                    }

                    // Now import the itf into temporary schema.
                    config.setXtffile(itfFile.getAbsolutePath());
                    Ili2db.runImport(config, "");

                    // Copy data into final schema and update additional
                    // attributes. 
                    // TODO: Do we need some rollback for temporary tables import
                    // when updating goes wrong?
                    // No: 'updateCommunity' is safe. We will just import the next
                    // one and will find some errors in the log file.
                    updateCommunity(fosnr, lot, deliveryDate);

                } catch (StringIndexOutOfBoundsException | NumberFormatException e) {
                    e.printStackTrace();
                    failedImports.add(entry.toAbsolutePath().toString());
                    logger.error(e.getMessage());
                    logger.error("Error while importing data.");
                    logger.error("File name is not valid: " + entry.getFileName().toString());
                    logger.error("Will continue with next file.");
                } catch (Exception e) {
//                } catch (Ili2dbException e) {
                    e.printStackTrace();
                    failedImports.add(entry.toAbsolutePath().toString());
                    logger.error(e.getMessage());
                    logger.error("Error while importing data: " + entry.getFileName().toString());
                    logger.error("Will continue with next file.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            logger.error("All imports failed. Cannot read directory: " + importDirPath);
        }

        return failedImports;
    }

    private void updateCommunity(int fosnr, int lot, Date deliveryDate) throws SQLException, Exception {
        // Get all table names and all attributes of a table from the 
        // final schema we have to deal with.
        HashMap<String, String[]> tables = getDataTablesInformation();

        if (tables.isEmpty()) {
            throw new Exception("Error getting data tables information from database.\nReturned list is empty.");
        }

        Class.forName("org.postgresql.Driver");

        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        int res;

        try {
            con = DriverManager.getConnection(dburl);
            con.setAutoCommit(false);

            for (String tableName : tables.keySet()) {
                logger.trace(tableName + " :: " + tables.get(tableName));

                st = con.createStatement();
                String sql = null;

                // Delete data
                sql = "\n"
                        + "DELETE FROM " + dbschema + "." + tableName + "\n"
                        + "WHERE gem_bfs = " + fosnr + "\n"
                        + "AND los = " + lot + ";";

                logger.trace(sql);

                res = st.executeUpdate(sql);

                // Copy data from temporay tables to final tables. 
                String[] attributes = tables.get(tableName);

                // We need two different strings with the attributes.
                // One with all the attributes (= target)
                // and one where we substitute 'gem_bfs', 'los' and
                // 'lieferdatum'.
                String attrSourceStr = new String();
                String attrTargetStr = new String();

                for (int i = 0; i < attributes.length; i++) {
                    String attrName = attributes[i];

                    if (attrName.equalsIgnoreCase("gem_bfs")) {
                        attrSourceStr += fosnr;
                    } else if (attrName.equalsIgnoreCase("los")) {
                        attrSourceStr += lot;
                    } else if (attrName.equalsIgnoreCase("lieferdatum")) {
                        java.sql.Timestamp sqlDeliveryDate = new java.sql.Timestamp((deliveryDate).getTime());
                        attrSourceStr += "'" + sqlDeliveryDate + "'";
                    } else {
                        attrSourceStr += attributes[i];
                    }

                    attrTargetStr += attributes[i];

                    if (i != attributes.length - 1) {
                        attrSourceStr += ", ";
                        attrTargetStr += ", ";
                    }
                }

                sql = "\n"
                        + "INSERT INTO " + dbschema + "." + tableName + " "
                        + "("
                        + attrTargetStr
                        + ")\n"
                        + "SELECT "
                        + attrSourceStr + "\n"
                        + "FROM " + dbschemaTmp + "." + tableName + ";";

                logger.trace(sql);

                res = st.executeUpdate(sql);
            }

            // Commit 'delete' and 'insert into' in one transaction for all
            // tables for one community.
            con.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new SQLException(e.getMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
                throw new SQLException(e.getMessage());            
            }
        }
    }

    private HashMap getDataTablesInformation() {
        HashMap<String, String[]> tables = new HashMap<String, String[]>();

        try {
            Class.forName("org.postgresql.Driver");

            Connection con = null;
            Statement st = null;
            ResultSet rs = null;

            String sql = null;

            // This query returns all tables that have the three additional
            // attributes (gem_bfs, los, lieferdatum). -> These are the tables
            // we need to deal with.
            sql = "\n"
                    + "SELECT tabs.table_name, attr.attributes\n"
                    + "FROM\n"
                    + "(\n"
                    + " SELECT table_name\n"
                    + " FROM\n"
                    + " (\n"
                    + "  SELECT count(col.column_name), col.table_name\n"
                    + "  FROM information_schema.tables as tab, information_schema.columns as col\n"
                    + "  WHERE tab.table_schema = '" + dbschema + "'\n"
                    + "  AND col.table_schema = '" + dbschema + "'\n"
                    + "  AND tab.table_name = col.table_name\n"
                    + "  AND col.column_name IN ('gem_bfs', 'los', 'lieferdatum')\n"
                    + "  GROUP BY col.table_name\n"
                    + " ) as t\n"
                    + " WHERE count = 3\n"
                    + ") as tabs,\n"
                    + "(\n"
                    + " SELECT table_name, array_agg(column_name::TEXT) as attributes\n"
                    + " FROM information_schema.columns\n"
                    + " WHERE table_schema = '" + dbschema + "'\n"
                    + " GROUP BY table_name\n"
                    + ") as attr\n"
                    + "WHERE tabs.table_name = attr.table_name;";

            logger.trace("Data tables query: " + sql);

            try {
                con = DriverManager.getConnection(dburl);
                st = con.createStatement();
                rs = st.executeQuery(sql);

                while (rs.next()) {
                    String table_name = rs.getString("table_name");
                    String[] attributes = (String[]) rs.getArray("attributes").getArray();

                    tables.put(table_name, attributes);
                }

            } catch (SQLException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (st != null) {
                        st.close();
                    }
                    if (con != null) {
                        con.close();
                    }

                } catch (SQLException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                }
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

        return tables;
    }

    private File unzipItf(Path zippedFile) {
        File newFile = null;

        // TODO: read about the buffer size.                
        byte[] buffer = new byte[2048];

        try {
            FileInputStream fis = new FileInputStream(zippedFile.toString());
            ZipInputStream zis = new ZipInputStream(fis);

            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {
                String fileName = ze.getName();

                // Check if it is an itf/ITF file. 
                String fileExtension = FilenameUtils.getExtension(fileName);
                if (!fileExtension.equalsIgnoreCase("itf")) {
                    logger.error("No itf found in zipped file: " + zippedFile.toString());
                    continue;
                }

                // We write the unzipped file (=itf) in the same directory.
                newFile = new File(zippedFile.getParent().toString() + File.separatorChar + fileName);

                FileOutputStream fos = new FileOutputStream(newFile);
                int count = 0;

                while ((count = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, count);
                }

                fos.close();

                zis.closeEntry();
                ze = zis.getNextEntry();
            }

            zis.closeEntry();
            zis.close();
            fis.close();

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Error while unzipping file: " + zippedFile.toString());
            logger.error(e.getMessage());
        }

        return newFile;
    }

    private void grantSchemaTables() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");

        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

        String sql = null;

        sql = "GRANT USAGE ON SCHEMA " + dbschema + " TO " + dbusrro + ";\n"
                + "GRANT SELECT ON ALL TABLES IN SCHEMA " + dbschema + " TO " + dbusrro + ";";

        try {
            con = DriverManager.getConnection(dburl);
            st = con.createStatement();
            int res = st.executeUpdate(sql);

            logger.debug("Grant usage/select update result: " + res);

        } catch (SQLException e) {
            logger.error(e.getMessage());

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }

            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
