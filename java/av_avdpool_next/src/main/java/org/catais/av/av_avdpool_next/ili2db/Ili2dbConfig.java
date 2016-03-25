/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.catais.av.av_avdpool_next.ili2db;

import ch.ehi.sqlgen.generator.SqlConfiguration;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author stefan
 */
public class Ili2dbConfig {
    static final Logger logger = LogManager.getLogger(Ili2dbConfig.class.getName());

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

    public Ili2dbConfig(HashMap<String, String> params) {
        dbhost = params.get("dbhost");
        dbport = params.get("dbport");
        dbdatabase = params.get("dbdatabase");
        dbusr = params.get("dbusr");
        dbpwd = params.get("dbpwd");
        defaultSrsAuth = params.get("defaultSrsAuth");
        defaultSrsCode = params.get("defaultSrsCode");
        modeldir = params.get("modeldir");
        loglevel = params.get("loglevel");
        //dburl = "jdbc:postgresql://" + dbhost + ":" + dbport + "/" + dbdatabase + "?user=" + dbusr + "&password=" + dbpwd;
    }
    
    public ch.ehi.ili2db.gui.Config getConfig(String dbschema, String models) {
        ch.ehi.ili2db.gui.Config config = new ch.ehi.ili2db.gui.Config();

        config.setDbhost(dbhost);
        config.setDbdatabase(dbdatabase);
        config.setDbport(dbport);
        config.setDbusr(dbusr);
        config.setDbpwd(dbpwd);
        config.setDburl("jdbc:postgresql://" + dbhost + ":" + dbport + "/" + dbdatabase);
        config.setDbschema(dbschema);
        config.setModels(models);
        config.setModeldir(modeldir);

        config.setGeometryConverter(ch.ehi.ili2pg.converter.PostgisColumnConverter.class.getName());
        config.setDdlGenerator(ch.ehi.sqlgen.generator_impl.jdbc.GeneratorPostgresql.class.getName());
        config.setJdbcDriver("org.postgresql.Driver");
        config.setIdGenerator(ch.ehi.ili2pg.PgSequenceBasedIdGen.class.getName());
        config.setUuidDefaultValue("uuid_generate_v4()");

        config.setNameOptimization(config.NAME_OPTIMIZATION_TOPIC);
        config.setMaxSqlNameLength("60");
        config.setStrokeArcs(config.STROKE_ARCS_ENABLE);

        config.setSqlNull(config.SQL_NULL_ENABLE); 
        config.setValue(SqlConfiguration.CREATE_GEOM_INDEX, "True");
        config.setTidHandling(config.TID_HANDLING_PROPERTY);
        config.setCreateFkIdx(config.CREATE_FKIDX_YES);
        config.setCreateEnumDefs(config.CREATE_ENUM_DEFS_MULTI);

        config.setDefaultSrsAuthority(defaultSrsAuth);
        config.setDefaultSrsCode(defaultSrsCode);
        
        return config;
    }
}
