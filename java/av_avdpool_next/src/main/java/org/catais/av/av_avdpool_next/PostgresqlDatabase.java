/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.catais.av.av_avdpool_next;

import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.base.Ili2dbException;
import ch.ehi.ili2db.gui.Config;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    HashMap<String,String> params = null;
    
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
    // TODO: Proper exception handling:
    // There will be no rollback if one of the three tasks fail.
    // So we can simple abort the whole process if there is an exception 
    // thrown.
    // Yes?
    public void initSchema() throws Ili2dbException, ClassNotFoundException, SQLException {
        // Create final schema
        Ili2dbConfig ili2dbConfig = new Ili2dbConfig(params);
        Config config = ili2dbConfig.getConfig(this.dbschema, this.models);
        Ili2db.runSchemaImport(config, "");

        // Grant schema and tables to public user.
        grantSchemaTables();
        
        // Create temporary schema
        config = ili2dbConfig.getConfig(this.dbschemaTmp, this.modelsTmp);
        Ili2db.runSchemaImport(config, "");
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
