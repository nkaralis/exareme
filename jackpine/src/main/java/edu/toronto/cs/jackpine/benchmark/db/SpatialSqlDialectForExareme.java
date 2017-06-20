package edu.toronto.cs.jackpine.benchmark.db;

import com.continuent.bristlecone.benchmark.db.SqlDialectForExareme;
import edu.toronto.cs.jackpine.benchmark.scenarios.macroscenario.VisitScenario;

public class SpatialSqlDialectForExareme extends SqlDialectForExareme implements SpatialSqlDialect{

	private int SRID = 4326;
	
	@Override
	public SupportedSqlDialect getSqlDialectType() {
		return SupportedSqlDialect.Exareme;
	}

	@Override
	public String getSelectAllFeaturesWithinADistanceFromPoint() {
		StringBuffer sb = new StringBuffer();

	    // Generate the join SQL statement
	    sb.append("select pk_uid from arealm_merge where ST_Distance(geomcol, ST_GeomFromText('POINT(-97.7 30.30)')) < 1000");
	    String sql = sb.toString();
	    return sql;
	}

	@Override
	public String getSelectTotalLength() { // den exei spatial function sto where
		StringBuffer sb = new StringBuffer();

		// Generate the join SQL statement.
		sb.append("SELECT sum(ST_Length(geomcol))/1000 AS km_roads FROM edges_merge");
		String sql = sb.toString();
		return sql;
		
		
	}

	@Override
	public String getSelectTotalArea() { // den exei spatial function sto where
		StringBuffer sb = new StringBuffer();

	    // Generate the join SQL statement.
	    sb.append("SELECT sum(ST_Area(geomcol)) AS area FROM areawater_merge");
	    String sql = sb.toString();
	    return sql;
	}

	@Override
	public String getSelectLongestLine() { // den exei spatial function sto where
		StringBuffer sb = new StringBuffer();

		// Generate the join SQL statement.
		sb.append("SELECT  gid, ST_Length(geomcol) AS line FROM edges_merge order BY line DESC limit 1");
		String sql = sb.toString();
		return sql;
	}

	@Override
	public String getSelectLargestArea() { // den exei spatial function sto where
		StringBuffer sb = new StringBuffer();

		// Generate the join SQL statement.
		sb.append("SELECT  gid, ST_Area(geomcol) AS area FROM areawater_merge order BY area DESC limit 1");
		String sql = sb.toString();
		return sql;
		
	}

	@Override
	public String getSelectDimensionPolygon() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSelectBufferPolygon() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSelectConvexHullPolygon() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSelectEnvelopeLine() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSelectBoundingBoxSearch() {
		StringBuffer sb = new StringBuffer();

	    // Generate the join SQL statement.
	    sb.append("select count(*) from edges_merge");
	    sb.append("  where ST_Within(geomcol, ST_GeomFromText('POLYGON((-97.7 30.30, -92.7 30.30, -92.7 27.30, -97.7 27.30, -97.7 30.30))'))");
	    String sql = sb.toString();
	    return sql;
	}

	@Override
	public String[] getSelectLongestLineIntersectsArea() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getSelectLineIntersectsLargestArea() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getSelectAreaOverlapsLargestArea() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getSelectLargestAreaContainsPoint() {
		// TODO Auto-generated method stub
		return null;
	}
	
/////////////////////////////////////////POLYGON AND POLYGON //////////////////////////////////////////////

	@Override
	/** Spatial Join Overlaps(Polygon, Polygon) */
	public String getSelectAreaOverlapsArea() { //DONE(2)
		StringBuffer sb = new StringBuffer();
		
		//sb.append("select a1.pk_uid, a2.pk_uid from  arealm_merge a1 , arealm_merge a2 where ST_Overlaps(a1.geomcol, a2.geomcol) and a2.rowid in "
		//		+ "(select rowid from spatialindex where f_table_name = 'arealm_merge' and search_frame = a1.geomcol)");
		sb.append("select a1.pk_uid, a2.pk_uid from  arealm_merge a1 , arealm_merge a2, spatialindex s where ST_Overlaps(a1.geomcol, a2.geomcol) "
				+ "and a2.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = a1.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	/** Spatial Join Contains(Polygon, Polygon) */
	public String getSelectAreaContainsArea() { //DONE(2)
		StringBuffer sb = new StringBuffer();
		  
		sb.append("select a1.pk_uid, a2.pk_uid from  arealm_merge a1 , arealm_merge a2 , spatialindex s where ST_Contains(a1.geomcol, a2.geomcol) "
				+ "and a2.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = a1.geomcol");
		String sql = sb.toString();
	  	return sql;
	}

	@Override
	/** Spatial Join Within(Polygon, Polygon) */
	public String getSelectAreaWithinArea() { //DONE(2)
		StringBuffer sb = new StringBuffer();
		  
		sb.append("select a1.pk_uid, a2.pk_uid from  arealm_merge a1 , arealm_merge a2 , spatialindex s where ST_Within(a1.geomcol, a2.geomcol) "
				+ "and a2.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = a1.geomcol");
		String sql = sb.toString();
	  	return sql;
	}

	@Override
	/** Spatial Join Touches(Polygon, Polygon) */
	public String getSelectAreaTouchesArea() { //DONE(2)
		StringBuffer sb = new StringBuffer();
		  
		sb.append("select a1.pk_uid, a2.pk_uid from  arealm_merge a1 , arealm_merge a2 , spatialindex s where ST_Touches(a1.geomcol, a2.geomcol) "
				+ "and a2.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = a1.geomcol");
		String sql = sb.toString();
	  	return sql;
	}

	@Override
	/** Spatial Join Equals(Polygon, Polygon) */
	public String getSelectAreaEqualsArea() { //DONE(2)
		StringBuffer sb = new StringBuffer();
		
		sb.append("select a1.pk_uid, a2.pk_uid from  arealm_merge a1 , arealm_merge a2 , spatialindex s where ST_Equals(a1.geomcol, a2.geomcol) "
				+ "and a2.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = a1.geomcol");
	  	String sql = sb.toString();
	  	return sql;
	}

	@Override
	/** Spatial Join Disjoint(Polygon, Polygon) */
	public String getSelectAreaDisjointArea() { 
		StringBuffer sb = new StringBuffer();
		
		sb.append("select a1.pk_uid, a2.pk_uid from  arealm_merge a1 , arealm_merge a2 , spatialindex s where ST_Disjoint(a1.geomcol, a2.geomcol) "
				+ "and a2.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = a1.geomcol");
		String sql = sb.toString();
		return sql;
	}

	///////////////////////////////////////LINE AND POLYGON//////////////////////////////////////////////
	@Override
	/** Spatial Join Intersects(Line, Polygon) */
	public String getSelectLineIntersectsArea() {
		StringBuffer sb = new StringBuffer();

		// Generate the join SQL statement.
		sb.append("select e.pk_uid, a.pk_uid from  arealm_merge a, edges_merge e , spatialindex s where ST_Intersects(e.geomcol, a.geomcol) "
				+ "and a.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = e.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	/** Spatial Join Crosses(Line, Polygon) */
	public String getSelectLineCrossesArea() {
		StringBuffer sb = new StringBuffer();
		  
		sb.append("select e.pk_uid, a.pk_uid from  arealm_merge a, edges_merge e , spatialindex s where ST_Crosses(e.geomcol, a.geomcol) "
				+ "and a.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = e.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	/** Spatial Join Within(Line, Polygon) */
	public String getSelectLineWithinArea() {
		StringBuffer sb = new StringBuffer();
		  
		sb.append("select e.pk_uid, a.pk_uid from  arealm_merge a, edges_merge e , spatialindex s where ST_Within(e.geomcol, a.geomcol) "
				+ "and a.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = e.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	/** Spatial Join Touches(Line, Polygon) */
	public String getSelectLineTouchesArea() {
		StringBuffer sb = new StringBuffer();
		  
		sb.append("select e.pk_uid, a.pk_uid from  arealm_merge a, edges_merge e , spatialindex s where ST_Touches(e.geomcol, a.geomcol) "
				+ "and a.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = e.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	/** Spatial Join Overlaps(Line, Polygon) */
	public String getSelectLineOverlapsArea() {
		StringBuffer sb = new StringBuffer();

		// Generate the join SQL statement.
		sb.append("select e.pk_uid, a.pk_uid from  arealm_merge a, edges_merge e , spatialindex s where ST_Overlaps(e.geomcol, a.geomcol) "
				+ "and a.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = e.geomcol");
		String sql = sb.toString();
		return sql;
	}
///////////////////////////////////////LINE AND LINE//////////////////////////////////////////////
	@Override
	/** Spatial Join Overlaps(Line, Line) */
	public String getSelectLineOverlapsLine() {
		StringBuffer sb = new StringBuffer();

		// Generate the join SQL statement. 
		sb.append("select e1.pk_uid  from  edges_merge e1 , edges_merge e2 , spatialindex s where ST_Overlaps(e1.geomcol, e2.geomcol) "
				+ "and e2.rowid = s.rowid and s.f_table_name = 'edges_merge' and s.search_frame = e1.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	/** Spatial Join Crosses(Line, Line) */
	public String getSelectLineCrossesLine() {
		StringBuffer sb = new StringBuffer();

		// Generate the join SQL statement. 
		sb.append("select e1.pk_uid  from  edges_merge e1 , edges_merge e2 , spatialindex s where ST_Crosses(e1.geomcol, e2.geomcol) "
				+ "and e2.rowid = s.rowid and s.f_table_name = 'edges_merge' and s.search_frame = e1.geomcol");
		String sql = sb.toString();
		return sql;
	}
///////////////////////////////////////POINT AND //////////////////////////////////////////////
	@Override
	/** Spatial Join Equals(Point, Point) */
	public String getSelectPointEqualsPoint() { //DONE(2)
		StringBuffer sb = new StringBuffer();
		sb.append("select p1.pk_uid, p2.pk_uid from  pointlm_merge p1, pointlm_merge p2 , spatialindex s where ST_Equals(p1.geomcol, p2.geomcol) "
				+ "and p2.rowid = s.rowid and s.f_table_name = 'pointlm_merge' and s.search_frame = p1.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	/** Spatial Join Within(Point, Area) */
	public String getSelectPointWithinArea() { //DONE(2)
		StringBuffer sb = new StringBuffer();

		// Generate the join SQL statement.
		sb.append("select a.pk_uid, p.pk_uid from arealm_merge a, pointlm_merge p, spatialindex s where ST_Within(p.geomcol, a.geomcol) "
				+ "and a.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = p.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	/** Spatial Join Intersects(Point, Area) */
	public String getSelectPointIntersectsArea() { //DONE(2)
		StringBuffer sb = new StringBuffer();
		sb.append("select a.pk_uid, p.pk_uid from arealm_merge a, pointlm_merge p, spatialindex s where ST_Intersects(p.geomcol, a.geomcol) "
				+ "and a.rowid = s.rowid and s.f_table_name = 'arealm_merge' and s.search_frame = p.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	/** Spatial Join Intersects(Point, Line) */
	public String getSelectPointIntersectsLine() { //DONE(2)
		StringBuffer sb = new StringBuffer();
		sb.append("select e.pk_uid, p.pk_uid from  edges_merge e, pointlm_merge p, spatialindex s where ST_Intersects(p.geomcol, e.geomcol) "
				+ "and p.rowid = s.rowid and s.f_table_name = 'edge_merge' and s.search_frame = e.geomcol");
		String sql = sb.toString();
		return sql;
	}

	@Override
	public String getMaxRowidFromSpatialTablePointlmMerge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getMaxRowidFromSpatialTableEdgesMerge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getMaxRowidFromSpatialTableArealmMerge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getInsertIntoEdgesMerge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getInsertIntoArealmMerge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getInsertIntoPointlmMerge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSpatialWriteCleanupEdgesMerge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSpatialWriteCleanupArealmMerge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSpatialWriteCleanupPointlmMerge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCityStateForReverseGeocoding() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStreetAddressForReverseGeocoding() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getGeocodingQuery() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getMapSearchSiteSearchQuery(VisitScenario visitScenario) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getMapSearchScenarioQueries(VisitScenario visitScenario) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getMapBrowseBoundingBoxQueries() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getLandUseQueries() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getEnvHazardQueries() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSpillPointIntersectsStreams() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSpilledDownstreamStreams() {
		// TODO Auto-generated method stub
		return null;
	}

}
