/**
 * Jackpine Spatial Database Benchmark 
 *  Copyright (C) 2010 University of Toronto
 * 
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Developer: S. Ray
 * Contributor(s): 
 */

package  edu.toronto.cs.jackpine.benchmark.db;

import com.continuent.bristlecone.benchmark.db.SqlDialectForInformix;

import edu.toronto.cs.jackpine.benchmark.db.SpatialSqlDialect.SupportedSqlDialect;
import edu.toronto.cs.jackpine.benchmark.scenarios.macroscenario.VisitScenario;

/**
 * Informix DBMS spatial dialect information. 
 * 
 * @author  sray
 */
public class SpatialSqlDialectForInformix extends SqlDialectForInformix implements SpatialSqlDialect
{
	 
  /**
   * 
   * @return
   */
  public SupportedSqlDialect getSqlDialectType() {
	  return SupportedSqlDialect.Informix;
  }
  
  
  public String getSelectAllFeaturesWithinADistanceFromPoint()
  {
	    StringBuffer sb = new StringBuffer();
	
	    sb.append("select count(*) from arealm_merge  where ST_distance(shape, ST_PointFromText('POINT(-97.7 30.30)', 0)) < 1000");	    
	    String sql = sb.toString();
	    return sql;
  }
  
  public String getSelectBoundingBoxSearch(){ 
	  
	  StringBuffer sb = new StringBuffer();

	    // Generate the join SQL statement.
	    sb.append("select count(*) from edges_merge");
	    sb.append(" where ST_Within( shape, ST_PolyFromText('POLYGON((-97.7 30.30, -92.7 30.30, -92.7 27.30, -97.7 27.30, -97.7	30.30))', 0))");
	    String sql = sb.toString();
	    return sql;
  }
  
  public String getSelectDimensionPolygon(){
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("select ST_Dimension(a.shape) from  arealm_merge a");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String getSelectBufferPolygon(){
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("select ST_Buffer(a.shape,5280) from  arealm_merge a");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String getSelectConvexHullPolygon(){
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("select ST_ConvexHull(a.shape) from  arealm_merge a");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String getSelectEnvelopeLine(){
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("select first 1000 ST_AsText(ST_Envelope(e.shape)) from  edges_merge e");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String getSelectLongestLine(){
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("SELECT first 1 se_row_id, ST_Length(shape) AS line FROM edges_merge ORDER BY line DESC");
	  String sql = sb.toString();
	  return sql;
	  
  }
  
  public  String getSelectLargestArea(){ 
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("SELECT first 1 se_row_id, ST_Area(shape) AS area FROM areawater_merge order BY area DESC");
	  String sql = sb.toString();
	  return sql;
  }
  
  public  String getSelectTotalLength(){ 
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("SELECT sum(ST_Length(shape))/1000 AS km_roads FROM edges_merge");
	  String sql = sb.toString();
	  return sql;
  }
 
  
  public String getSelectTotalArea(){
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("SELECT sum(ST_Area(shape)) AS area FROM areawater_merge");
	  String sql = sb.toString();
	  return sql;
	  
  }
  
 ////////////////////////////////////////SPATIAL JOIN//////////////////////////////////////////////////
  // all the polygons that are intersected by the longest line: 1
  public String[] getSelectLongestLineIntersectsArea(){ 
	  String[] queries =  new String[2]; 
	  queries[0] = "select first 1 se_row_id from edges_merge order by ST_Length(shape) desc";
	  queries[1] = "select count(*) from areawater_merge a, edges_merge e where ST_Intersects(e.shape, a.shape) and e.se_row_id= ?";		  
	  return queries;
  }
  
  //all the lines that intersect the largest area: 371
  public String[] getSelectLineIntersectsLargestArea(){ 
	  String[] queries =  new String[2]; 
	  queries[0] = "select first 1 se_row_id from arealm_merge order by ST_Area(shape) desc";
	  queries[1] = "select count(*) from  arealm_merge a, edges_merge e where ST_Intersects(e.shape, a.shape) and a.se_row_id= ?";	  
	  return queries;
  }
  
  
  //all the area overlaps the largest area:
  public String[] getSelectAreaOverlapsLargestArea(){ 
	  String[] queries =  new String[2]; 
	  queries[0] = "select first 1 se_row_id from arealm_merge order by ST_Area(shape) desc";
	  queries[1] = "select count(*) from  arealm_merge al, areawater_merge aw where ST_Overlaps(al.shape, aw.shape) and al.se_row_id= ?";		  
	  return queries;
  
  }
  
  //all the point within the largest area:
  public String[] getSelectLargestAreaContainsPoint(){ 
	  String[] queries =  new String[2]; 
	  queries[0] = "select first 1 se_row_id from areawater_merge order by ST_Area(shape) desc";
	  queries[1] = "select count(*) from pointlm_merge pl, areawater_merge aw where ST_Contains(aw.shape, pl.shape) and aw.se_row_id= ?";		  
	  return queries;
  
  }
 
  ////////////////////////////////////////ALLPAIR SPATIAL JOIN//////////////////////////////////////////////////
  
  ///////////////////////////////////////POLYGON AND POLYGON//////////////////////////////////////////////
  
  public String getSelectAreaOverlapsArea(){ 
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_overlaps(a1.shape, a2.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String getSelectAreaContainsArea(){  
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_contains(a1.shape, a2.shape)");
	  String sql = sb.toString();
  	  return sql;
  }
  
  public String getSelectAreaWithinArea(){ 
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_within(a1.shape, a2.shape)");
	  String sql = sb.toString();
  	  return sql;
  }
  
  public String getSelectAreaTouchesArea(){ 
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_touches(a1.shape, a2.shape)");
	  String sql = sb.toString();
  	  return sql;
  }
  
  public String getSelectAreaEqualsArea(){ 
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  arealm_merge a1 , arealm_merge a2 where ST_equals(a1.shape, a2.shape)");
	  String sql = sb.toString();
  	  return sql;
  }
  
  public String getSelectAreaDisjointArea(){ 
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  arealm_merge a1, arealm_merge a2 where ST_Disjoint(a1.shape, a2.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  ///////////////////////////////////////LINE AND POLYGON//////////////////////////////////////////////
  
  public  String getSelectLineIntersectsArea(){
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("select  count(*) from  arealm_merge a, edges_merge e where ST_Intersects(e.shape, a.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  public  String getSelectLineOverlapsArea(){ 
	  StringBuffer sb = new StringBuffer();
    
	  // Generate the join SQL statement.
	  sb.append("select  count(*) from  arealm_merge a, edges_merge e where ST_Overlaps(e.shape, a.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String getSelectLineCrossesArea(){
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  arealm_merge a, edges_merge e where ST_crosses(e.shape, a.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String getSelectLineWithinArea(){
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  arealm_merge a, edges_merge e where ST_within(e.shape, a.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String getSelectLineTouchesArea(){
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  arealm_merge a, edges_merge e where ST_Touches(e.shape, a.shape)");
	  String sql = sb.toString();
	  return sql; 
  }
  
  
  ///////////////////////////////////////LINE AND LINE//////////////////////////////////////////////
  
  public String getSelectLineOverlapsLine(){ 
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement. 
	  sb.append("select first 5  e1.se_row_id from  edges_merge e1 , edges_merge e2 where ST_overlaps(e1.shape, e2.shape)  ");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String getSelectLineCrossesLine(){ 
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement. 
	  sb.append("select first 5  e1.se_row_id from  edges_merge e1 , edges_merge e2 where ST_crosses(e1.shape, e2.shape)  ");
	  String sql = sb.toString();
	  return sql;
  }
  
  ///////////////////////////////////////POINT AND //////////////////////////////////////////////
  
    
  public  String getSelectPointWithinArea(){ 
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("select count(*)  from  arealm_merge a, pointlm_merge p where ST_Within(p.shape, a.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  public  String getSelectPointIntersectsArea(){
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*)  from  arealm_merge a, pointlm_merge p where ST_Intersects(p.shape, a.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  public  String getSelectPointIntersectsLine(){
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*)  from  edges_merge e, pointlm_merge p where ST_Intersects(p.shape, e.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  public  String getSelectPointEqualsPoint(){
	  StringBuffer sb = new StringBuffer();
	  sb.append("select count(*) from  pointlm_merge p1, pointlm_merge p2 where ST_Equals(p1.shape, p2.shape)");
	  String sql = sb.toString();
	  return sql;
  }
  
  
  //////////////////////////////////////////INSERT RECORDS/////////////////////////////////
  
  java.util.Random rnd = new java.util.Random(System.currentTimeMillis());
   
  public String[] getInsertIntoPointlmMerge(){
	  
	  
    String sqlStmts[] =  new String[10];
	  
	  StringBuffer sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43954 31.581223)')");	 	 
	  sqlStmts[0] = sb.toString();
	  
	  
	  sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43954 31.581224)')");	 	 
	  sqlStmts[1] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43955 31.581223)')");	 	 
	  sqlStmts[2] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43954 31.581225)')");	  
	  sqlStmts[3] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43956 31.581223)')");	  
	  sqlStmts[4] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43954 31.581226)')");	 
	  sqlStmts[5] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43957 31.581223)')");	 	 
	  sqlStmts[6] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43954 31.581227)')");	 	 
	  sqlStmts[7] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43958 31.581223)')");	 	 
	  sqlStmts[8] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into pointlm_merge (se_row_id,  statefp, countyfp, ansicode, pointid, fullname, mtfcc, shape ) values (?,'48','001','','','TEMP','', '0 point(-95.43954 31.581228)')");	 	 
	  sqlStmts[9] = sb.toString();
	  
	  return sqlStmts;
	  
  }
  
  public String getSpatialWriteCleanupPointlmMerge() {
	  StringBuffer sb = new StringBuffer();
	  sb.append("delete from pointlm_merge where fullname='TEMP'");
	  String sql = sb.toString();
	  return sql;
  }
  
  public String[] getInsertIntoEdgesMerge(){
	  
	  
	  String sqlStmts[] =  new String[10];
	  
	  StringBuffer sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43954 31.581223,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')");	 
	  sqlStmts[0] = sb.toString();
	  
	  
	  sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43954 31.581224,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')");	 
	  sqlStmts[1] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43955 31.581223,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')");	 
	  sqlStmts[2] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43954 31.581225,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')"); 
	  sqlStmts[3] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43956 31.581223,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')"); 
	  sqlStmts[4] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43954 31.581226,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')");
	  sqlStmts[5] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43957 31.581223,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')");	 
	  sqlStmts[6] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43954 31.581227,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')");	 
	  sqlStmts[7] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43958 31.581223,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')");	 
	  sqlStmts[8] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into edges_merge (se_row_id,statefp,countyfp,tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, ltoadd, rfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, shape ) values (?,'48','001',74690317,205838157,205838661,'P0002','TEMP','447','','','','','','','','N','N','N','N','','','N','','','','','N','N','0',14062338,14062338,  '0 linestring(-95.43954 31.581228,-95.439587 31.581385,-95.439555 31.581396,-95.439535 31.58139,-95.439452 31.581401,-95.439433 31.581412,-95.439433 31.581445,-95.439426 31.581463,-95.439407 31.581473,-95.439386 31.581471,-95.439337 31.581445,-95.439285 31.581434,-95.439273 31.581418,-95.439274 31.5814,-95.439292 31.581385,-95.43933 31.581374,-95.439337 31.581346,-95.439329 31.581329,-95.439285 31.581269,-95.439215 31.581214,-95.439164 31.581187,-95.439144 31.581165,-95.439086 31.581121,-95.438952 31.581077,-95.438759 31.581028,-95.438714 31.581011,-95.438676 31.580989,-95.438643 31.580962,-95.438611 31.580912,-95.438586 31.580813,-95.438566 31.580797,-95.438277 31.580709,-95.438263 31.580695,-95.438252 31.580676,-95.438245 31.580643,-95.438207 31.580593,-95.438181 31.580577,-95.438149 31.580566,-95.438104 31.58056,-95.438078 31.580533,-95.438073 31.580515,-95.43813 31.580478,-95.438277 31.580434,-95.438432  31.580401)')");	 
	  sqlStmts[9] = sb.toString();
	  
	  return sqlStmts;
  }
  
  public String getSpatialWriteCleanupEdgesMerge() {
	  StringBuffer sb = new StringBuffer();
	  sb.append("delete from edges_merge where fullname='TEMP'");
	  String sql = sb.toString(); 
	  return sql;
  }
  
  public String getMaxRowidFromSpatialTablePointlmMerge(){
  	return "select max(se_row_id) from pointlm_merge";
  
  }
  
  public String getMaxRowidFromSpatialTableEdgesMerge(){
	  return "select max(se_row_id) from edges_merge";
  
  }
  
  public String getMaxRowidFromSpatialTableArealmMerge(){
	  return "select max(se_row_id) from arealm_merge";
  
  }
  
  public String[] getInsertIntoArealmMerge(){
	   
	  
	  String sqlStmts[] =  new String[10];
	  
	  StringBuffer sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758691,-95.83335 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')");
	  sqlStmts[0] = sb.toString();
	  
	  
	  sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758692,-95.83335 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')");
	  sqlStmts[1] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758691,-95.83336 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')");
	  sqlStmts[2] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758693,-95.83335 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')");
	  sqlStmts[3] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758691,-95.83337 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')"); 
	  sqlStmts[4] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758694,-95.83335 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')");
	  sqlStmts[5] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758691,-95.83338 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')");
	  sqlStmts[6] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758695,-95.83335 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')");	 
	  sqlStmts[7] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758691,-95.83339 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')");	 
	  sqlStmts[8] = sb.toString();
	  
	  sb = new StringBuffer();
	  sb.append("insert into arealm_merge (se_row_id,statefp, countyfp, ansicode, areaid, fullname, mtfcc, shape) values (?,'48','001','','110223506483','TEMP','K1237', '0 polygon ((-95.835919 31.758732,-95.83358 31.758696,-95.83335 31.758687,-95.831252 31.75865,-95.831252 31.760502,-95.831243 31.761303,-95.830347 31.761285,-95.829896 31.760892,-95.829178 31.760883,-95.827652 31.760878,-95.827168 31.760874,-95.821293 31.76082,-95.81972 31.760805,-95.815488 31.760741,-95.815307 31.760741,-95.815244 31.76045,-95.815173 31.760274,-95.814993 31.759966,-95.814987 31.759873,-95.814935 31.759774,-95.81489 31.759625,-95.814852 31.759307,-95.814807 31.759043,-95.814877 31.758592,-95.814903 31.758328,-95.814948 31.758081,-95.814967 31.757839,-95.815077 31.757718,-95.815373 31.757427,-95.815424 31.757328,-95.81545 31.757235,-95.815437 31.757153,-95.815373 31.757043,-95.815038 31.756592,-95.814987 31.756471,-95.81498 31.756411,-95.814993 31.756345,-95.81516 31.755806,-95.815225 31.755707,-95.81527 31.755586,-95.815295 31.75546,-95.815276 31.755202,-95.81534 31.754801,-95.815347 31.75457,-95.815289 31.754394,-95.815237 31.754317,-95.815077 31.754163,-95.814967 31.754042,-95.814877 31.753916,-95.814832 31.753833,-95.814832 31.75374,-95.814852 31.753581,-95.81489 31.75341,-95.814942 31.753267,-95.815019 31.753152,-95.815115 31.753048,-95.815225 31.752965,-95.81543 31.752866,-95.815675 31.752668,-95.815758 31.752614,-95.815842 31.75257,-95.816016 31.75252,-95.816099 31.752476,-95.816196 31.752416,-95.816466 31.752201,-95.816569 31.752146,-95.816671 31.752135,-95.816916 31.752157,-95.817006 31.752152,-95.817173 31.752086,-95.817707 31.751778,-95.817868 31.751701,-95.818208 31.751569,-95.818311 31.751575,-95.818369 31.751569,-95.818434 31.751547,-95.818498 31.751509,-95.818594 31.751421,-95.818633 31.751366,-95.818691 31.751201,-95.818755 31.75113,-95.818813 31.751053,-95.818839 31.750965,-95.818871 31.750762,-95.818909 31.750679,-95.818974 31.750619,-95.819012 31.750597,-95.81907 31.750586,-95.819218 31.75058,-95.819302 31.750613,-95.819359 31.750624,-95.819533 31.750619,-95.819752 31.750553,-95.819829 31.750448,-95.819784 31.750311,-95.819677 31.750171,-95.836184 31.758403,-95.836133 31.758518,-95.836133 31.758579,-95.836152 31.758667,-95.836175 31.75874,-95.835919 31.758732))')");
	  sqlStmts[9] = sb.toString();
	  
	  return sqlStmts;
	 
	  
  }
  
  public String getSpatialWriteCleanupArealmMerge() {
	  StringBuffer sb = new StringBuffer();
	  sb.append("delete from arealm_merge where fullname='TEMP'");
	  String sql = sb.toString();
	  return sql;
  }
 
//////////////////////////////////////////MACRO BENCHMARK: REVERSE GEOCODING/////////////////////////////////
  
  public  String getCityStateForReverseGeocoding(){ 
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("SELECT first 1 name, st, st_distance(shape, ST_GeomFromText(?,0)) as dist FROM cityinfo order by dist ");
	  String sql = sb.toString();

	  return sql;
  }
  
  public  String getStreetAddressForReverseGeocoding(){ 
	  StringBuffer sb = new StringBuffer();

	  // Generate the join SQL statement.
	  sb.append("SELECT first 1 fullname,lfromadd,ltoadd,rfromadd,rtoadd,zipl,zipr, st_distance(shape, ST_GeomFromText(?,0 ))  as d FROM edges_merge   where St_Intersects(shape, ST_GeomFromText(?,0))    and st_distance(shape, ST_GeomFromText(?,0 )) < 0.1   and  roadflg='Y' order by d  ");
	  String sql = sb.toString();

	  return sql;
  }
  
  //////////////////////////////////////////MACRO BENCHMARK: GEOCODING/////////////////////////////////
  
  public  String getGeocodingQuery(){ 
	 
	  String sql = "select t.tlid, t.fraddr, t.fraddl, t.toaddr, t.toaddl,"+ 
	  " t.zipL, t.zipR, t.tolat, t.tolong, t.frlong, t.frlat,"+  
	  " t.long1, t.lat1, t.long2, t.lat2, t.long3, t.lat3, t.long4, t.lat4,"+
	  " t.long5, t.lat5, t.long6, t.lat6, t.long7, t.lat7, t.long8, t.lat8,"+
	  " t.long9, t.lat9, t.long10, t.lat10, t.fedirp, t.fetype, t.fedirs from " +
	  " geocoder_address "+
	  " t where t.fename = ? and "+
	  "(" + 
	  "       (t.fraddL <= ? and t.toaddL >= ?) or (t.fraddL >= ? and t.toaddL <= ?) "+
	  "    or (t.fraddR <= ? and t.toaddR >= ?) or (t.fraddR >= ? and t.toaddR <= ?) "+
	  ")" +  
	  "  and (t.zipL = ? or t.zipR = ?)";

	  return sql;
  }
  
  
  //////////////////////////////////////////MACRO BENCHMARK: MAP SEARCH AND BROWSE /////////////////////////////////
  
  public String getMapSearchSiteSearchQuery(VisitScenario scenario) {
  	
  	// the landmark itself
  	String query = " select first 1 se_row_id, name, shape as location from gnis_names09  where state='TX' and name='"+scenario.getChosenPoiName()+"' and class='"+scenario.getChosenPoiClassName()+"' ";
  					 
  	return query;
  }
 
  public String[] getMapSearchScenarioQueries(VisitScenario scenario) {
  	String[] queries =  new String[scenario.getTotalVisitSearches()];
  	int queryIndex=-1;
  	
  	String[] nearestEssentialPoiClassOnceMatchStrings=scenario.getNearestEssentialPoiClassOnceMatchStrings();
  	if (nearestEssentialPoiClassOnceMatchStrings!=null && nearestEssentialPoiClassOnceMatchStrings.length!=0) {
  		for (int i=0;i<nearestEssentialPoiClassOnceMatchStrings.length;i++) {
  			queryIndex++;
  			String classOnce= nearestEssentialPoiClassOnceMatchStrings[i];
  			//?s: lon lat lon lat
  			queries[queryIndex]= " select first 1 name, shape as location, ST_Distance(shape, ST_GeomFromText(?, 0 )) as dist from gnis_names09  where state='TX' and class like '%"+classOnce+"%' and (ST_Distance(shape, ST_GeomFromText(?, 0 ))  <= "+VisitScenario.MAX_SEARCH_RADIUS+") and se_row_id <> ? order by dist ";																			 
  		}
  	}
  	
  	String[] nearestEssentialPoiNameOnceMatchStrings=scenario.getNearestEssentialPoiNameOnceMatchStrings();
  	if (nearestEssentialPoiNameOnceMatchStrings!=null && nearestEssentialPoiNameOnceMatchStrings.length!=0 ) {
  		for (int i=0;i<nearestEssentialPoiNameOnceMatchStrings.length;i++) {
  			queryIndex++;
  			String nameOnce= nearestEssentialPoiNameOnceMatchStrings[i];
  			//?s: lon lat lon lat
  			queries[queryIndex]= " select first 1 name, shape as location, ST_Distance(shape, ST_GeomFromText(?, 0 )) as dist from gnis_names09  where state='TX' and name like '%"+nameOnce+"%' and (ST_Distance(shape, ST_GeomFromText(?, 0 ))  <= "+VisitScenario.MAX_SEARCH_RADIUS+") and se_row_id <> ? order by dist ";	                     
  		}
  	}
  	
  	String[] nearestEssentialPoiNameOrMatchStrings= scenario.getNearestEssentialPoiNameOrMatchStrings();
  	if (nearestEssentialPoiNameOrMatchStrings!=null && nearestEssentialPoiNameOrMatchStrings.length!=0) {
  		String nameOrMatchingStrings = "";
  		for (int i=0;i<nearestEssentialPoiNameOrMatchStrings.length;i++) {
  			String nameOr= nearestEssentialPoiNameOrMatchStrings[i];
  			if (i>0)
  				nameOrMatchingStrings	+=" or ";
  			nameOrMatchingStrings += "name like '%"+nameOr+"%'";
  		}
  		//?s: lon lat lon lat
  		queryIndex++;
		queries[queryIndex]= " select first 1 name, shape as location, ST_Distance(shape, ST_GeomFromText(?, 0 )) as dist from gnis_names09  where state='TX' and ("+nameOrMatchingStrings+") and (ST_Distance(shape, ST_GeomFromText(?, 0 ))  <= "+VisitScenario.MAX_SEARCH_RADIUS+") and se_row_id <> ? order by dist ";							 
  	}
  	
  	int limit=scenario.getNearestEssentialPoiClassOrMatchOptionsNum();
  	  
  	String[] nearestEssentialPoiClassOrMatchStrings= scenario.getNearestEssentialPoiClassOrMatchStrings();
  	if (nearestEssentialPoiClassOrMatchStrings!=null && nearestEssentialPoiClassOrMatchStrings.length!=0) {
  		String classOrMatchingStrings = "";
  		for (int i=0;i<nearestEssentialPoiClassOrMatchStrings.length;i++) {
  			String classOr= nearestEssentialPoiClassOrMatchStrings[i];
  			if (i>0)
  				classOrMatchingStrings	+=" or ";
  			classOrMatchingStrings += "class like '%"+classOr+"%'";
  		}
  		//?s: lon lat lon lat
  		queryIndex++;
		queries[queryIndex]= " select first "+limit+" name, shape as location, ST_Distance(shape, ST_GeomFromText(?, 0 )) as dist from gnis_names09  where state='TX' and ("+classOrMatchingStrings+") and (ST_Distance(shape, ST_GeomFromText(?, 0 ))  <= "+VisitScenario.MAX_SEARCH_RADIUS+") and se_row_id <> ? order by dist ";							         
  	}
  	
  	limit=scenario.getNearestOptionalPoiNameOrMatchOptionsNum();
  	
  	String[] nearestOptionalPoiNameOrMatchStrings= scenario.getNearestOptionalPoiNameOrMatchStrings();
  	if (nearestOptionalPoiNameOrMatchStrings!=null && nearestOptionalPoiNameOrMatchStrings.length!=0) {
  		String nameOrMatchingStrings = "";
  		for (int i=0;i<nearestOptionalPoiNameOrMatchStrings.length;i++) {
  			String nameOr= nearestOptionalPoiNameOrMatchStrings[i];
  			if (i>0)
  				nameOrMatchingStrings	+=" or ";
  			nameOrMatchingStrings += "name like '%"+nameOr+"%'";
  		}
  		//?s: lon lat lon lat
  		queryIndex++;
			queries[queryIndex]= " select first "+limit+" name, shape as location, ST_Distance(shape, ST_GeomFromText(?, 0 )) as dist from gnis_names09  where state='TX' and ("+nameOrMatchingStrings+") and (ST_Distance(shape, ST_GeomFromText(?, 0 ))  <= "+VisitScenario.MAX_SEARCH_RADIUS+") and se_row_id <> ? order by dist ";										 
  	}
  	
  	String[] nearestOptionalPoiClassAnyMatchStrings= scenario.getNearestOptionalPoiClassOrMatchStrings();
  	if (nearestOptionalPoiClassAnyMatchStrings!=null && nearestOptionalPoiClassAnyMatchStrings.length!=0) {
  		String classOrMatchingStrings = "";
  		for (int i=0;i<nearestOptionalPoiClassAnyMatchStrings.length;i++) {
  			String classOr= nearestOptionalPoiClassAnyMatchStrings[i];
  			if (i>0)
  				classOrMatchingStrings	+=" or ";
  			classOrMatchingStrings += "class like '%"+classOr+"%'";
  		}
  		//?s: lon lat lon lat
  		queryIndex++;
			queries[queryIndex]= " select first 1 name, shape as location, ST_Distance(shape, ST_GeomFromText(?, 0 )) as dist from gnis_names09  where state='TX' and ("+classOrMatchingStrings+") and (ST_Distance(shape, ST_GeomFromText(?, 0 ))  <= "+VisitScenario.MAX_SEARCH_RADIUS+") and se_row_id <> ? order by dist ";
  	}
  	
  	return queries;
  }
  
  public String[] getMapBrowseBoundingBoxQueries() {
  	
  	// 5 queries
  	String[] queries =  new String[5];       
  				  
  	queries[0] = "SELECT se_row_id,ST_AsBinary(shape) as shp FROM gnis_names09 WHERE ST_Intersects(ST_GeomFromText(?, 0), shape)";
  	queries[1] = "SELECT se_row_id,ST_AsBinary(shape) as shp FROM arealm_merge WHERE ST_Intersects(ST_GeomFromText(?, 0), shape)";
  	queries[2] = "SELECT se_row_id,ST_AsBinary(shape) as shp FROM areawater_merge WHERE ST_Intersects(ST_GeomFromText(?, 0), shape)";
  	queries[3] = "SELECT se_row_id,ST_AsBinary(shape) as shp FROM pointlm_merge WHERE ST_Intersects(ST_GeomFromText(?, 0), shape)";
  	queries[4] = "SELECT se_row_id,ST_AsBinary(shape) as shp FROM edges_merge WHERE ST_Intersects(ST_GeomFromText(?, 0), shape)";
  	
  	
  	return queries;
  }
  
  //////////////////////////////////////////MACRO BENCHMARK: LAND USE /////////////////////////////////
  public String[] getLandUseQueries() { 
	  
	
		//6 queries
		String[] queries =  new String[6]; 
		// queries
		
		// What is the average property value per sq foot for Single-Family Residential properties
		queries[0] = "select sum(pa.marketvalu)/sum(st_area(pa.shape)) from parcels2008 as pa where pa.land_state like 'A%'";
		  
		//How many residential properties have a  hospital within one mile radius in Austin 
		queries[1] = "select count(*) from  land_use_2006 as lu, hospitals as h where lu.general_la in (100,113,150,160,200) and  ST_Distance(lu.shape, h.shape) <= 5280.0 ";
					  
		// Determine the average property values  within a one mile radius of all hospitals in Travis county 
		queries[2] = "select sc.name, avg(pa.marketvalu) as avg_property_value from hospitals as sc, parcels2008 as pa  where ST_Distance(sc.shape, pa.shape) <= 5280.0 group by sc.name order by avg_property_value desc ";

		
		//Which office buildings have front yard parking restrictions 
		queries[3] = "select property_i from  land_use_2006 as lu, frontyard_parking_restrictions fypr where lu.general_la=400  and  ST_Overlaps(fypr.shape, lu.shape) ";
					  
				
		// Show the commercial properties that are built on un-permitted landfills
		queries[4] = "select lu.property_i from  land_use_2006 as lu, landfills lf where lu.general_la=300 and lf.permitted='UNPERMITTED' and ST_Intersects(lf.shape,lu.shape) ";
				
		
		//Rewritten query: Waterfront properties: show all the property values sorted by price within a 100 feet of the three major lakes in Travis county (first 10 matches)
		queries[5] = "select first 10 lake.wtr_nm, geo_id, pa.marketvalu as property_value from parcels2008 as pa, s_wtr_ar as lake where  ST_Distance(lake.shape, pa.shape) <= 100.0 and pa.marketvalu> 0 ";
	   	
		return queries;
  }
  
  //////////////////////////////////////////MACRO BENCHMARK: Flood Risk /////////////////////////////////
  public String[] getEnvHazardQueries() { 
	    
	    // 4 queries
		String[] queries =  new String[4]; 
		
		//Ok - Show the flood risk areas that are protected by one or more dams.
		queries[0] = "select distinct fld_ar_id from s_gen_struct st, s_fld_haz_ar fa where st.struct_typ='DAM' and ST_Intersects(st.shape, fa.shape) ";
		  			  			  
		
		//Ok - Show the total flood risk area in acres  grouped by risk area category
		queries[1] = "select fld_zone, sum(ST_Area(s_fld_haz_ar.shape)/43560) as area from s_fld_haz_ar group by fld_zone ";
					  
		// Which residential property owners are requires to carry flood insurance?
		queries[2] = "select first 10 lu.property_i from  land_use_2006 as lu,  s_fld_haz_ar fz where lu.general_la in (100,113,150,160,200) and (fld_zone='A' or fld_zone='AE' or fld_zone='AO' or fld_zone='V') and ST_Overlaps(lu.shape, fz.shape)  ";
		
		//rewritten; wait: Which industrial complexes are in high risk flood area?
		queries[3] = "select first 10 lu.property_i from  land_use_2006 as lu,  s_fld_haz_ar fz where lu.general_la=500 and (fld_zone='A' or fld_zone='AE' or fld_zone='AO' or fld_zone='V') and ST_Overlaps(lu.shape, fz.shape) ";
		
	return queries;
  }
  
 //////////////////////////////////////////MACRO BENCHMARK: Spill scenario //////////////////////////////////////////
  // need to create an index on hydroflg
  // CREATE INDEX idx_hydroflg ON edges_merge (hydroflg); 
  
  // rivers that intersect a point
  
  public String getSpillPointIntersectsStreams() { 
	  String query = "select se_row_id from edges_merge  where  hydroflg = 'Y' AND ST_Intersects(shape, ST_PointFromText('POINT(-95.753361 31.636858)', 0))";

	  return query;
  }
  
  // all the rivers within 20 mile downstream
  public String getSpilledDownstreamStreams() { 
	  String query = "select e2.se_row_id from edges_merge e1, edges_merge e2 where e2.hydroflg = 'Y' AND e1.se_row_id=? and Equals(ST_StartPoint(e2.shape), ST_EndPoint(e1.shape)) AND ST_Distance(ST_StartPoint(e2.shape),  ST_PointFromText('POINT(-95.753361 31.636858)', 0) )  <= 32187";
	  
	  return query;
  }
}