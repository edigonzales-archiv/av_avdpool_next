cppt:

SELECT t_id, 'LFP1'::text as kategorie, nummer, geometrie, hoehegeom AS hoehe, punktzeichen, gem_bfs as bfsnr, 
    round(ST_X(geometrie)::numeric,3) || ' / ' || round(ST_Y(geometrie)::numeric, 3) as koordinate
  FROM av_avdpool_next.fixpunktekatgrie1_lfp1

  UNION ALL

  SELECT t_id, 'HFP1'::text as kategorie, nummer, geometrie, hoehegeom AS hoehe, 'weitere' as punktzeichen, gem_bfs as bfsnr,
    round(ST_X(geometrie)::numeric,3) || ' / ' || round(ST_Y(geometrie)::numeric, 3) as koordinate
  FROM av_avdpool_next.fixpunktekatgrie1_hfp1

  UNION ALL
  
  SELECT t_id, 'LFP2'::text as kategorie, nummer, geometrie, hoehegeom AS hoehe, punktzeichen, gem_bfs as bfsnr,
    round(ST_X(geometrie)::numeric,3) || ' / ' || round(ST_Y(geometrie)::numeric, 3) as koordinate  
  FROM av_avdpool_next.fixpunktekatgrie2_lfp2

  UNION ALL

  SELECT t_id, 'HFP2'::text as kategorie, nummer, geometrie, hoehegeom AS hoehe, 'weitere' as punktzeichen, gem_bfs as bfsnr,
    round(ST_X(geometrie)::numeric,3) || ' / ' || round(ST_Y(geometrie)::numeric, 3) as koordinate  
  FROM av_avdpool_next.fixpunktekatgrie2_hfp2

  UNION ALL
  
  SELECT t_id, 'LFP3'::text as kategorie, nummer, geometrie, hoehegeom AS hoehe, punktzeichen, gem_bfs as bfsnr,
    round(ST_X(geometrie)::numeric,3) || ' / ' || round(ST_Y(geometrie)::numeric, 3) as koordinate  
  FROM av_avdpool_next.fixpunktekatgrie3_lfp3

  UNION ALL

  SELECT t_id, 'HFP3'::text as kategorie, nummer, geometrie, hoehegeom AS hoehe, 'weitere' as punktzeichen, gem_bfs as bfsnr,
    round(ST_X(geometrie)::numeric,3) || ' / ' || round(ST_Y(geometrie)::numeric, 3) as koordinate  
  FROM av_avdpool_next.fixpunktekatgrie3_hfp3


lcsfproj:

SELECT t_id, qualitaet, art, gem_bfs as bfsnr, NULL::int as gwr_egid, geometrie
FROM av_avdpool_next.bodenbedeckung_projboflaeche


lcsf:

SELECT t_id, qualitaet, gem_bfs as bfsnr, NULL::int as gwr_egid, geometrie,
CASE
  WHEN art LIKE 'Gebaeude' THEN 'Gebaeude'  
  WHEN art LIKE 'befestigt.Strasse_Weg%' THEN 'Strasse_Weg'  
  WHEN art LIKE 'befestigt.Trottoir%' THEN 'Trottoir'  
  WHEN art LIKE 'befestigt.Verkehrsinsel%' THEN 'Verkehrsinsel'  
  WHEN art LIKE 'befestigt.Bahn%' THEN 'Bahn'  
  WHEN art LIKE 'befestigt.Flugplatz%' THEN 'Flugplatz'  
  WHEN art LIKE 'befestigt.Wasserbecken%' THEN 'Wasserbecken' 
  WHEN art LIKE 'befestigt.uebrige_befestigte%' THEN 'uebrige_befestigte'
  WHEN art LIKE 'humusiert.Acker_Wiese_Weide%' THEN 'Acker_Wiese_Weide'  
  WHEN art LIKE 'humusiert.Intensivkultur.Reben%' THEN 'Reben'  
  WHEN art LIKE 'humusiert.Intensivkultur.uebrige_Intensivkultur%' THEN 'uebrige_Intensivkultur'
  WHEN art LIKE 'humusiert.Gartenanlage%' THEN 'Gartenanlage'
  WHEN art LIKE 'humusiert.Hoch_Flachmoor%' THEN 'Hoch_Flachmoor'
  WHEN art LIKE 'Gewaesser.stehendes%' THEN 'stehendes'
  WHEN art LIKE 'Gewaesser.fliessendes%' THEN 'fliessendes'
  WHEN art LIKE 'Gewaesser.Schilfguertel%' THEN 'Schilfguertel'
  WHEN art LIKE 'bestockt.geschlossener_Wald%' THEN 'geschlossener_Wald'
  WHEN art LIKE 'bestockt.Wytweide.Wytweide_dicht%' THEN 'Wytweide_dicht'
  WHEN art LIKE 'bestockt.Wytweide.Wytweide_offen%' THEN 'Wytweide_offen'
  WHEN art LIKE 'bestockt.uebrige_bestockte%' THEN 'uebrige_bestockte'
  WHEN art LIKE 'vegetationslos.Fels%' THEN 'Fels'
  WHEN art LIKE 'vegetationslos.Gletscher_Firn%' THEN 'Gletscher_Firn'
  WHEN art LIKE 'vegetationslos.Geroell_Sand%' THEN 'Geroell_Sand'
  WHEN art LIKE 'vegetationslos.Abbau_Deponie%' THEN 'Abbau_Deponie'
  WHEN art LIKE 'vegetationslos.uebrige_vegetationslose%' THEN 'uebrige_vegetationslose'
END AS art
FROM av_avdpool_next.bodenbedeckung_boflaeche


lcobj:

SELECT  b.t_id, a.gem_bfs AS bfsnr, 'Name'::text AS typ, a.aname AS text, b.ori, b.pos, (100::double precision - b.ori) * 0.9::double precision AS rot, b.hali, b.vali
FROM av_avdpool_next.bodenbedeckung_objektname a, av_avdpool_next.bodenbedeckung_objektnamepos b
WHERE a.gem_bfs = b.gem_bfs 
AND a.t_id::text = b.objektnamepos_von::text 


sofs:
SELECT b.t_id, a.gem_bfs AS bfsnr, 'gueltig'::text AS gueltigkeit, NULL::int as gwr_egid, b.geometrie,
CASE
  WHEN art LIKE 'Mauer%' THEN 'Mauer'  
  WHEN art LIKE 'unterirdisches_Gebaeude' THEN 'unterirdisches_Gebaeude'  
  WHEN art LIKE 'uebriger_Gebaeudeteil' THEN 'uebriger_Gebaeudeteil'  
  WHEN art LIKE 'eingedoltes_oeffentliches_Gewaesser' THEN 'eingedoltes_oeffentliches_Gewaesser'  
  WHEN art LIKE 'wichtige_Treppe' THEN 'wichtige_Treppe'  
  WHEN art LIKE 'Tunnel_Unterfuehrung_Galerie%' THEN 'Tunnel_Unterfuehrung_Galerie'  
  WHEN art LIKE 'Bruecke_Passerelle' THEN 'Bruecke_Passerelle'  
  WHEN art LIKE 'Bahnsteig%' THEN 'Bahnsteig'  
  WHEN art LIKE 'Brunnen' THEN 'Brunnen'  
  WHEN art LIKE 'Reservoir' THEN 'Reservoir'  
  WHEN art LIKE 'Pfeiler' THEN 'Pfeiler'  
  WHEN art LIKE 'Unterstand' THEN 'Unterstand'  
  WHEN art LIKE 'Silo_Turm_Gasometer' THEN 'Silo_Turm_Gasometer'  
  WHEN art LIKE 'Hochkamin' THEN 'Hochkamin'  
  WHEN art LIKE 'Denkmal' THEN 'Denkmal'  
  WHEN art LIKE 'Mast_Antenne%' THEN 'Mast_Antenne'  
  WHEN art LIKE 'Aussichtsturm' THEN 'Aussichtsturm'  
  WHEN art LIKE 'Uferverbauung' THEN 'Uferverbauung'  
  WHEN art LIKE 'Schwelle' THEN 'Schwelle'  
  WHEN art LIKE 'Lawinenverbauung' THEN 'Lawinenverbauung'  
  WHEN art LIKE 'massiver_Sockel' THEN 'massiver_Sockel'  
  WHEN art LIKE 'Ruine_archaeologisches_Objekt' THEN 'Ruine_archaeologisches_Objekt'  
  WHEN art LIKE 'Landungssteg' THEN 'Landungssteg'  
  WHEN art LIKE 'einzelner_Fels' THEN 'einzelner_Fels'  
  WHEN art LIKE 'schmale_bestockte_Flaeche' THEN 'schmale_bestockte_Flaeche'  
  WHEN art LIKE 'Rinnsal' THEN 'Rinnsal'  
  WHEN art LIKE 'schmaler_Weg%' THEN 'schmaler_Weg'  
  WHEN art LIKE 'Hochspannungsfreileitung' THEN 'Hochspannungsfreileitung'  
  WHEN art LIKE 'Druckleitung' THEN 'Druckleitung'  
  WHEN art LIKE 'Bahngeleise%' THEN 'Bahngeleise'  
  WHEN art LIKE 'Luftseilbahn' THEN 'Luftseilbahn'  
  WHEN art LIKE 'Gondelbahn_Sesselbahn' THEN 'Gondelbahn_Sesselbahn'  
  WHEN art LIKE 'Materialseilbahn' THEN 'Materialseilbahn'  
  WHEN art LIKE 'Skilift' THEN 'Skilift'  
  WHEN art LIKE 'Faehre' THEN 'Faehre'  
  WHEN art LIKE 'Grotte_Hoehleneingang' THEN 'Grotte_Hoehleneingang'  
  WHEN art LIKE 'Achse%' THEN 'Achse'  
  WHEN art LIKE 'wichtiger_Einzelbaum' THEN 'wichtiger_Einzelbaum'  
  WHEN art LIKE 'Bildstock_Kruzifix' THEN 'Bildstock_Kruzifix'  
  WHEN art LIKE 'Quelle' THEN 'Quelle'  
  WHEN art LIKE 'Bezugspunkt' THEN 'Bezugspunkt'  
  WHEN art LIKE 'weitere%' THEN 'weitere'  
END AS art
FROM av_avdpool_next.einzelobjekte_einzelobjekt as a, av_avdpool_next.einzelobjekte_flaechenelement as b
WHERE a.gem_bfs = b.gem_bfs
AND a.t_id = b.flaechenelement_von


soli:
SELECT b.t_id, a.gem_bfs AS bfsnr, 'gueltig'::text AS gueltigkeit, b.geometrie,
CASE
  WHEN art LIKE 'Mauer%' THEN 'Mauer'  
  WHEN art LIKE 'unterirdisches_Gebaeude' THEN 'unterirdisches_Gebaeude'  
  WHEN art LIKE 'uebriger_Gebaeudeteil' THEN 'uebriger_Gebaeudeteil'  
  WHEN art LIKE 'eingedoltes_oeffentliches_Gewaesser' THEN 'eingedoltes_oeffentliches_Gewaesser'  
  WHEN art LIKE 'wichtige_Treppe' THEN 'wichtige_Treppe'  
  WHEN art LIKE 'Tunnel_Unterfuehrung_Galerie%' THEN 'Tunnel_Unterfuehrung_Galerie'  
  WHEN art LIKE 'Bruecke_Passerelle' THEN 'Bruecke_Passerelle'  
  WHEN art LIKE 'Bahnsteig%' THEN 'Bahnsteig'  
  WHEN art LIKE 'Brunnen' THEN 'Brunnen'  
  WHEN art LIKE 'Reservoir' THEN 'Reservoir'  
  WHEN art LIKE 'Pfeiler' THEN 'Pfeiler'  
  WHEN art LIKE 'Unterstand' THEN 'Unterstand'  
  WHEN art LIKE 'Silo_Turm_Gasometer' THEN 'Silo_Turm_Gasometer'  
  WHEN art LIKE 'Hochkamin' THEN 'Hochkamin'  
  WHEN art LIKE 'Denkmal' THEN 'Denkmal'  
  WHEN art LIKE 'Mast_Antenne%' THEN 'Mast_Antenne'  
  WHEN art LIKE 'Aussichtsturm' THEN 'Aussichtsturm'  
  WHEN art LIKE 'Uferverbauung' THEN 'Uferverbauung'  
  WHEN art LIKE 'Schwelle' THEN 'Schwelle'  
  WHEN art LIKE 'Lawinenverbauung' THEN 'Lawinenverbauung'  
  WHEN art LIKE 'massiver_Sockel' THEN 'massiver_Sockel'  
  WHEN art LIKE 'Ruine_archaeologisches_Objekt' THEN 'Ruine_archaeologisches_Objekt'  
  WHEN art LIKE 'Landungssteg' THEN 'Landungssteg'  
  WHEN art LIKE 'einzelner_Fels' THEN 'einzelner_Fels'  
  WHEN art LIKE 'schmale_bestockte_Flaeche' THEN 'schmale_bestockte_Flaeche'  
  WHEN art LIKE 'Rinnsal' THEN 'Rinnsal'  
  WHEN art LIKE 'schmaler_Weg%' THEN 'schmaler_Weg'  
  WHEN art LIKE 'Hochspannungsfreileitung' THEN 'Hochspannungsfreileitung'  
  WHEN art LIKE 'Druckleitung' THEN 'Druckleitung'  
  WHEN art LIKE 'Bahngeleise%' THEN 'Bahngeleise'  
  WHEN art LIKE 'Luftseilbahn' THEN 'Luftseilbahn'  
  WHEN art LIKE 'Gondelbahn_Sesselbahn' THEN 'Gondelbahn_Sesselbahn'  
  WHEN art LIKE 'Materialseilbahn' THEN 'Materialseilbahn'  
  WHEN art LIKE 'Skilift' THEN 'Skilift'  
  WHEN art LIKE 'Faehre' THEN 'Faehre'  
  WHEN art LIKE 'Grotte_Hoehleneingang' THEN 'Grotte_Hoehleneingang'  
  WHEN art LIKE 'Achse%' THEN 'Achse'  
  WHEN art LIKE 'wichtiger_Einzelbaum' THEN 'wichtiger_Einzelbaum'  
  WHEN art LIKE 'Bildstock_Kruzifix' THEN 'Bildstock_Kruzifix'  
  WHEN art LIKE 'Quelle' THEN 'Quelle'  
  WHEN art LIKE 'Bezugspunkt' THEN 'Bezugspunkt'  
  WHEN art LIKE 'weitere%' THEN 'weitere'  
END AS art
FROM av_avdpool_next.einzelobjekte_einzelobjekt as a, av_avdpool_next.einzelobjekte_linienelement as b
WHERE a.gem_bfs = b.gem_bfs
AND a.t_id = b.linienelement_von

sopt:
SELECT b.t_id, a.gem_bfs AS bfsnr, 'gueltig'::text AS gueltigkeit, b.geometrie,
CASE
  WHEN art LIKE 'Mauer%' THEN 'Mauer'  
  WHEN art LIKE 'unterirdisches_Gebaeude' THEN 'unterirdisches_Gebaeude'  
  WHEN art LIKE 'uebriger_Gebaeudeteil' THEN 'uebriger_Gebaeudeteil'  
  WHEN art LIKE 'eingedoltes_oeffentliches_Gewaesser' THEN 'eingedoltes_oeffentliches_Gewaesser'  
  WHEN art LIKE 'wichtige_Treppe' THEN 'wichtige_Treppe'  
  WHEN art LIKE 'Tunnel_Unterfuehrung_Galerie%' THEN 'Tunnel_Unterfuehrung_Galerie'  
  WHEN art LIKE 'Bruecke_Passerelle' THEN 'Bruecke_Passerelle'  
  WHEN art LIKE 'Bahnsteig%' THEN 'Bahnsteig'  
  WHEN art LIKE 'Brunnen' THEN 'Brunnen'  
  WHEN art LIKE 'Reservoir' THEN 'Reservoir'  
  WHEN art LIKE 'Pfeiler' THEN 'Pfeiler'  
  WHEN art LIKE 'Unterstand' THEN 'Unterstand'  
  WHEN art LIKE 'Silo_Turm_Gasometer' THEN 'Silo_Turm_Gasometer'  
  WHEN art LIKE 'Hochkamin' THEN 'Hochkamin'  
  WHEN art LIKE 'Denkmal' THEN 'Denkmal'  
  WHEN art LIKE 'Mast_Antenne%' THEN 'Mast_Antenne'  
  WHEN art LIKE 'Aussichtsturm' THEN 'Aussichtsturm'  
  WHEN art LIKE 'Uferverbauung' THEN 'Uferverbauung'  
  WHEN art LIKE 'Schwelle' THEN 'Schwelle'  
  WHEN art LIKE 'Lawinenverbauung' THEN 'Lawinenverbauung'  
  WHEN art LIKE 'massiver_Sockel' THEN 'massiver_Sockel'  
  WHEN art LIKE 'Ruine_archaeologisches_Objekt' THEN 'Ruine_archaeologisches_Objekt'  
  WHEN art LIKE 'Landungssteg' THEN 'Landungssteg'  
  WHEN art LIKE 'einzelner_Fels' THEN 'einzelner_Fels'  
  WHEN art LIKE 'schmale_bestockte_Flaeche' THEN 'schmale_bestockte_Flaeche'  
  WHEN art LIKE 'Rinnsal' THEN 'Rinnsal'  
  WHEN art LIKE 'schmaler_Weg%' THEN 'schmaler_Weg'  
  WHEN art LIKE 'Hochspannungsfreileitung' THEN 'Hochspannungsfreileitung'  
  WHEN art LIKE 'Druckleitung' THEN 'Druckleitung'  
  WHEN art LIKE 'Bahngeleise%' THEN 'Bahngeleise'  
  WHEN art LIKE 'Luftseilbahn' THEN 'Luftseilbahn'  
  WHEN art LIKE 'Gondelbahn_Sesselbahn' THEN 'Gondelbahn_Sesselbahn'  
  WHEN art LIKE 'Materialseilbahn' THEN 'Materialseilbahn'  
  WHEN art LIKE 'Skilift' THEN 'Skilift'  
  WHEN art LIKE 'Faehre' THEN 'Faehre'  
  WHEN art LIKE 'Grotte_Hoehleneingang' THEN 'Grotte_Hoehleneingang'  
  WHEN art LIKE 'Achse%' THEN 'Achse'  
  WHEN art LIKE 'wichtiger_Einzelbaum' THEN 'wichtiger_Einzelbaum'  
  WHEN art LIKE 'Bildstock_Kruzifix' THEN 'Bildstock_Kruzifix'  
  WHEN art LIKE 'Quelle' THEN 'Quelle'  
  WHEN art LIKE 'Bezugspunkt' THEN 'Bezugspunkt'  
  WHEN art LIKE 'weitere%' THEN 'weitere'  
END AS art, b.ori, (100 - b.ori) * 0.9 AS rot
FROM av_avdpool_next.einzelobjekte_einzelobjekt as a, av_avdpool_next.einzelobjekte_punktelement as b
WHERE a.gem_bfs = b.gem_bfs
AND a.t_id = b.punktelement_von

soobj:

SELECT  b.t_id, a.gem_bfs AS bfsnr, 'Name'::text AS typ, a.aname AS text, b.ori, b.pos, (100::double precision - b.ori) * 0.9::double precision AS rot, b.hali, b.vali
FROM av_avdpool_next.einzelobjekte_objektname a, av_avdpool_next.einzelobjekte_objektnamepos b
WHERE a.gem_bfs = b.gem_bfs 
AND a.t_id::text = b.objektnamepos_von::text 


lnna:
SELECT a.t_id, b.aname as name, a.pos, a.hali, a.vali,  a.ori, 
  (100 - ori) * 0.9 as rot, 'Flurname'::text AS kategorie
FROM av_avdpool_next.nomenklatur_flurnamepos as a, av_avdpool_next.nomenklatur_flurname as b
WHERE a.gem_bfs = b.gem_bfs
AND a.flurnamepos_von = b.t_id

UNION ALL

SELECT a.t_id, b.aname as name, a.pos, a.hali, a.vali,  a.ori, 
  (100 - ori) * 0.9 as rot, 'Gelaendename'::text AS kategorie
FROM av_avdpool_next.nomenklatur_gelaendenamepos as a, av_avdpool_next.nomenklatur_gelaendename as b
WHERE a.gem_bfs = b.gem_bfs
AND a.gelaendenamepos_von = b.t_id

UNION ALL

SELECT a.t_id, b.aname as name, a.pos, a.hali, a.vali,  a.ori, 
  (100 - ori) * 0.9 as rot, 'Ortsname'::text AS kategorie
FROM av_avdpool_next.nomenklatur_ortsnamepos as a, av_avdpool_next.nomenklatur_ortsname as b
WHERE a.gem_bfs = b.gem_bfs
AND a.ortsnamepos_von = b.t_id


resfproj:

SELECT a.t_id, a.gem_bfs AS bfsnr, b.nummer, b.nbident, b.egris_egrid, b.vollstaendigkeit, a.flaechenmass as flaeche, a.geometrie
FROM av_avdpool_next.liegenschaften_projliegenschaft as a, av_avdpool_next.liegenschaften_projgrundstueck as b
WHERE a.gem_bfs = b.gem_bfs
AND a.projliegenschaft_von = b.t_id

dprsfproj:

SELECT a.t_id, a.gem_bfs AS bfsnr, b.nummer, b.nbident, b.egris_egrid, b.vollstaendigkeit, a.flaechenmass as flaeche, a.geometrie
FROM av_avdpool_next.liegenschaften_projselbstrecht as a, av_avdpool_next.liegenschaften_projgrundstueck as b
WHERE a.gem_bfs = b.gem_bfs
AND a.projselbstrecht_von = b.t_id

osnrproj:

SELECT a.t_id, a.gem_bfs AS bfsnr, b.nbident, b.nummer, a.ori, (100 - a.ori) * 0.9 as rot, a.hali, a.vali, a.pos
FROM av_avdpool_next.liegenschaften_projgrundstueckpos as a, av_avdpool_next.liegenschaften_projgrundstueck as b
WHERE a.gem_bfs = b.gem_bfs
AND a.projgrundstueckpos_von = b.t_id

osbp:
SELECT a.t_id, a.gem_bfs AS bfsnr, b.gueltigkeit, a.punktzeichen, a.geometrie
FROM av_avdpool_next.liegenschaften_grenzpunkt as a, av_avdpool_next.liegenschaften_lsnachfuehrung as b
WHERE a.gem_bfs = b.gem_bfs
AND a.entstehung = b.t_id

resf:
SELECT a.t_id, a.gem_bfs AS bfsnr, b.nbident, b.nummer, b.egris_egrid, b.vollstaendigkeit, a.flaechenmass as flaeche, a.geometrie
FROM av_avdpool_next.liegenschaften_liegenschaft as a, av_avdpool_next.liegenschaften_grundstueck as b
WHERE a.gem_bfs = b.gem_bfs
AND a.liegenschaft_von = b.t_id

dprsf:

SELECT a.t_id, a.gem_bfs AS bfsnr, b.nummer, b.nbident, b.egris_egrid, b.vollstaendigkeit, a.flaechenmass as flaeche, a.geometrie
FROM av_avdpool_next.liegenschaften_selbstrecht as a, av_avdpool_next.liegenschaften_grundstueck as b
WHERE a.gem_bfs = b.gem_bfs
AND a.selbstrecht_von = b.t_id

osnr:
SELECT a.t_id, a.gem_bfs AS bfsnr, b.nbident, b.nummer, a.ori, (100 - a.ori) * 0.9 as rot, a.hali, a.vali, a.pos
FROM av_avdpool_next.liegenschaften_grundstueckpos as a, av_avdpool_next.liegenschaften_grundstueck as b
WHERE a.gem_bfs = b.gem_bfs
AND a.grundstueckpos_von = b.t_id

plli:
SELECT a.t_id, a.gem_bfs AS bfsnr, b.betreiber, b.art as medium, 'gueltig'::text as gueltigkeit, a.geometrie
FROM av_avdpool_next.rohrleitungen_linienelement as a, av_avdpool_next.rohrleitungen_leitungsobjekt as b
WHERE a.gem_bfs = b.gem_bfs
AND a.linienelement_von = b.t_id

plna:
SELECT a.t_id, a.gem_bfs AS bfsnr, b.betreiber, a.ori, (100 - a.ori) * 0.9 as rot, a.hali, a.vali, a.pos
FROM av_avdpool_next.rohrleitungen_leitungsobjektpos as a, av_avdpool_next.rohrleitungen_leitungsobjekt as b
WHERE a.gem_bfs = b.gem_bfs
AND a.leitungsobjektpos_von = b.t_id

tbbp:
SELECT a.t_id, a.gem_bfs AS bfsnr, b.gueltigkeit, a.punktzeichen, a.geometrie
FROM av_avdpool_next.gemeindegrenzen_hoheitsgrenzpunkt as a, av_avdpool_next.gemeindegrenzen_gemnachfuehrung as b
WHERE a.gem_bfs = b.gem_bfs
AND a.entstehung = b.t_id

mbsf:
SELECT b.t_id, a.gem_bfs AS bfsnr, a.aname AS name, b.geometrie
FROM av_avdpool_next.gemeindegrenzen_gemeinde as a, av_avdpool_next.gemeindegrenzen_gemeindegrenze as b
WHERE a.gem_bfs = b.gem_bfs
AND b.gemeindegrenze_von = a.t_id

tbli:
SELECT t_id, 'gueltig'::text AS gueltigkeit, 'Bezirksgrenzen'::text AS typ, geometrie
FROM av_avdpool_next.bezirksgrenzen_bezirksgrenzabschnitt

UNION ALL

SELECT t_id, 'gueltig'::text AS gueltigkeit, 'Kantonsgrenzen'::text AS typ, geometrie
FROM av_avdpool_next.kantonsgrenzen_kantonsgrenzabschnitt

UNION ALL

SELECT t_id, 'gueltig'::text AS gueltigkeit, 'Landesgrenzen'::text AS typ, geometrie
FROM av_avdpool_next.landesgrenzen_landesgrenzabschnitt

hadr:
SELECT a.t_id, a.gem_bfs AS bfsnr, b.hausnummer, b.gwr_egid, b.gwr_edid, a.pos, a.ori, (100 - a.ori) * 0.9 AS rot, a.hali, a.vali,
  NULL::text AS gebaeudename, NULL::text AS strassenname, NULL::int AS plz, NULL::int AS zusatzziffern, 
  NULL::text AS ortschaftsname
FROM av_avdpool_next.gebaeudeadressen_hausnummerpos as a, av_avdpool_next.gebaeudeadressen_gebaeudeeingang as b
WHERE a.gem_bfs = b.gem_bfs
AND a.hausnummerpos_von = b.t_id

locpos:
SELECT a.t_id, a.gem_bfs AS bfsnr, b.atext AS strassenname, a.ori, (100 - a.ori) * 0.9 AS rot, a.hali, a.vali, a.pos
FROM av_avdpool_next.gebaeudeadressen_lokalisationsnamepos as a, av_avdpool_next.gebaeudeadressen_lokalisationsname as b
WHERE a.gem_bfs = b.gem_bfs
AND a.lokalisationsnamepos_von = b.t_id