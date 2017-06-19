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

package edu.toronto.cs.jackpine.benchmark.scenarios;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;

import edu.toronto.cs.jackpine.benchmark.scenarios.macroscenario.*;

import org.apache.log4j.Logger;

import edu.toronto.cs.jackpine.benchmark.db.SpatialSqlDialect;


/**
 * Implements a scenario which finds all affected downstream streams/rivers during a  chemical/oil spill accident 
 * 
 * 
 * @author  sray
 */
public class SpillMacroScenario extends SpatialScenarioBase
{
  protected static final Logger logger = Logger.getLogger(SpillMacroScenario.class);

  
  protected PreparedStatement  spillPointIntersectsStreamsPstmt ;
  protected PreparedStatement  spilledDownstreamStreamsPstmt;
  protected HashSet<Integer> spilledStreamIdHs =  null;
  
  SpatialSqlDialect dialect = null;

  
  /** Create a prepared statement array. */
  public void prepare() throws Exception
  {
  
	  dialect = helper.getSpatialSqlDialect(); 

		 	 		  
	 // prepare the queries
	 String sql = dialect.getSpillPointIntersectsStreams();
	 logger.warn("Preparing: " + sql);
	 spillPointIntersectsStreamsPstmt  =  conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		 
	 sql = dialect.getSpilledDownstreamStreams();
	 logger.warn("Preparing: " + sql);
	 spilledDownstreamStreamsPstmt  =  conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY); 

  }

  /** Execute an interation. */
  public void iterate(long iterationCount) throws Exception
  {
	  spilledStreamIdHs = new HashSet();
 
	  findSpillPointIntersectsStreams();
	  
	  resultsetCount = RESULTSET_COUNT_NA;
	  /*
	  if (spilledStreamIdHs != null) {
		  resultsetCount = spilledStreamIdHs.size();
	  }
	  */
  }

  /** Clean up resources used by scenario. */
  public void cleanup() throws Exception
  {
	  if (spilledStreamIdHs != null) {
		  Iterator it = spilledStreamIdHs.iterator();
		  while (it.hasNext()) {
			  int sid = (Integer)it.next();
			  //logger.warn("Spilled stream id=" + sid );
			  System.out.print("," + sid );
		  }
	  } 
	  // Clean up connections. 
	  spillPointIntersectsStreamsPstmt.close();
	  spilledDownstreamStreamsPstmt.close();
	  
	  if (conn != null)
		  conn.close();
  }
  
  private void findSpilledDownstreamStreams(int recId) throws SQLException, ClassNotFoundException {
  
	  if (!spilledStreamIdHs.contains(recId)) {	  
		  spilledStreamIdHs.add(recId);
		  spilledDownstreamStreamsPstmt.setInt(1, recId);
		  logger.warn(spilledDownstreamStreamsPstmt.toString());
		  
		  ResultSet r = spilledDownstreamStreamsPstmt.executeQuery(); 
		  
		  ArrayList<Integer> recAL = new ArrayList();
		  while (r.next())   {
	  		 recId = r.getInt(1);	 //gid or ogr_fid
	  		 recAL.add(recId);
	  	  }
		  r.close();
		  
		  Iterator it = recAL.iterator();
		  while (it.hasNext()) {
			  int rid = (Integer)it.next();
			  findSpilledDownstreamStreams(rid); 
		  }
	  }
  }
  
  private void findSpillPointIntersectsStreams() throws SQLException, ClassNotFoundException
  {	  
	 logger.warn(spillPointIntersectsStreamsPstmt.toString()); 
  	 ResultSet r = spillPointIntersectsStreamsPstmt.executeQuery();
  	 int recId = 0; 
  	 ArrayList<Integer> recAL = new ArrayList();
  	 while (r.next())   {
  		 recId = r.getInt(1);	 //gid or ogr_fid
  		 recAL.add(recId);
  	 }
  	 r.close();
  	 Iterator it = recAL.iterator();
	 while (it.hasNext()) {
		 int rid = (Integer)it.next();
		 findSpilledDownstreamStreams(rid); 
	 }
  }
  
}
