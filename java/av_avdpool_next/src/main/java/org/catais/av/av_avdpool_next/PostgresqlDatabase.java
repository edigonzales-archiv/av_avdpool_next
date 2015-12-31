/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.catais.av.av_avdpool_next;

import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.base.Ili2dbException;
import ch.ehi.ili2db.gui.Config;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    String defaultSrsAuth = null;
    String defaultSrsCode = null;
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

    public PostgresqlDatabase(HashMap<String, String> params) {
        dbhost = params.get("dbhost");
        dbport = params.get("dbport");
        dbdatabase = params.get("dbdatabase");
        dbusr = params.get("dbusr");
        dbpwd = params.get("dbpwd");
        defaultSrsAuth = params.get("defaultSrsAuth");
        defaultSrsCode = params.get("defaultSrsCode");
        modeldir = params.get("modeldir");        
        loglevel = params.get("loglevel");
        dburl = "jdbc:postgresql://" + dbhost + ":" + dbport + "/" + dbdatabase + "?user=" + dbusr + "&password=" + dbpwd;
             
        dbschemaTmp = params.get("dbschemaTmp");
        modelsTmp = params.get("modelsTmp");

        dbschema = params.get("dbschema");
        models = params.get("models");
        
        dbusrro = params.get("dbusrro");
        dbpwdro = params.get("dbusrro");
    }

    public void initFinalSchema() throws Ili2dbException {        
        Config config = getConfig(this.dbschema, this.models);
        Ili2db.runSchemaImport(config, "");
    }
    
    public void initTempSchema() throws Ili2dbException {
        Config config = getConfig(this.dbschemaTmp, this.modelsTmp);
        Ili2db.runSchemaImport(config, "");
    }

    private Config getConfig(String dbschema, String models) {
        Config config = new Config();

        config.setDbhost(dbhost);
        config.setDbdatabase(dbdatabase);
        config.setDbport(dbport);
        config.setDbusr(dbusr);
        config.setDbpwd(dbpwd);
        config.setDburl(dburl);
        config.setDbschema(dbschema);
        config.setModels(models);
        config.setModeldir(modeldir);

        config.setGeometryConverter(ch.ehi.ili2pg.converter.PostgisColumnConverter.class.getName());
        config.setDdlGenerator(ch.ehi.sqlgen.generator_impl.jdbc.GeneratorPostgresql.class.getName());
        config.setJdbcDriver("org.postgresql.Driver");
        config.setIdGenerator(ch.ehi.ili2pg.PgSequenceBasedIdGen.class.getName());
        config.setUuidDefaultValue("uuid_generate_v4()");

        config.setNameOptimization("topic");
        config.setMaxSqlNameLength("60");
        config.setStrokeArcs("enable");

        config.setSqlNull("enable"); // be less restrictive
        config.setValue("ch.ehi.sqlgen.createGeomIndex", "True");
        config.setTidHandling(config.TID_HANDLING_PROPERTY);
        config.setCreateFkIdx(config.CREATE_FKIDX_YES);
        config.setCreateEnumDefs(config.CREATE_ENUM_DEFS_MULTI);

        config.setDefaultSrsAuthority(defaultSrsAuth);
        config.setDefaultSrsCode(defaultSrsCode);

        return config;
    }
}
