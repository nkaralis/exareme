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
import java.sql.Types;
import java.util.Properties;

import org.apache.log4j.Logger;

import edu.toronto.cs.jackpine.benchmark.db.SpatialSqlDialect;

/**
 * Implements a scenario that repeatedly inserts into one or more tables.  
 * Inserts are non-conflicting (i.e., should never deadlock).  <p>
 * 
 * @author sray
 */
public class WriteSpatialPointScenario extends SpatialScenarioBase
{
	  private static final Logger logger = Logger.getLogger(WriteSpatialPointScenario.class);

	  protected PreparedStatement[] pstmtArray;
	  protected PreparedStatement cleanUpPstmt;
	  
	  static final int MAXNUM=1000;
	  static final int SETTOTAL=10;
	  SpatialSqlDialect dialect = null;
	  static int maxRowId=-1;
	  
	  /** Create a prepared statement array. */
	  public void prepare() throws Exception
	  {
		  dialect = helper.getSpatialSqlDialect(); 
		    
		  if (dialect.getSqlDialectType() == SpatialSqlDialect.SupportedSqlDialect.Informix) {
			  String mriSql= dialect.getMaxRowidFromSpatialTablePointlmMerge();
			  PreparedStatement maxRowIdPstmt = conn.prepareStatement(mriSql);
			  //logger.warn(maxRowIdPstmt.toString());
			  

			  try {  
				   logger.warn(maxRowIdPstmt.toString());
				   ResultSet rs= maxRowIdPstmt.executeQuery();
				   if (rs.next()) {
					   maxRowId = rs.getInt(1);
					   //logger.warn("Max row count :" + maxRowId);
				   }
				   rs.close();
				   maxRowIdPstmt.close();
				}
				catch (Exception e) {
				   	e.printStackTrace();
				    logger.error(e.toString());
				   	throw new Exception("Cleanup WriteSpatialPointScenario");
				}  
			  
		  }

				 
		  int  cnt=0;
		  pstmtArray = new PreparedStatement[MAXNUM];
		  for (int i=0;i<MAXNUM/SETTOTAL;i++) {
		   	  String[] sql = dialect.getInsertIntoPointlmMerge();
		   	  for (int j=0;j<SETTOTAL;j++) {
		   		  pstmtArray[cnt] = conn.prepareStatement(sql[j]);
		   		  cnt++;
		   	  }
		  }
		  logger.warn("*** Total records to insert" + cnt);
		    
		  String sql = dialect.getSpatialWriteCleanupPointlmMerge();
		  cleanUpPstmt = conn.prepareStatement(sql);
		  
		  try {  
			  logger.warn(cleanUpPstmt.toString());	
			  cleanUpPstmt.executeUpdate();
			}
		    catch (Exception e) {
		    	e.printStackTrace();
		    	 logger.debug(e.toString());
		    	throw new Exception("Cleanup WriteSpatialPointScenario");
		    }  
	  }

	  /** Execute an interation. */
	  public void iterate(long iterationCount) throws Exception
	  {
	  
	    try {
		int index = 0;
		for (int i=0;i<MAXNUM;i++) {
			PreparedStatement pstmt = pstmtArray[index];
			if (dialect.getSqlDialectType() == SpatialSqlDialect.SupportedSqlDialect.Informix) {
				pstmt.setInt(1, ++maxRowId);
			}
			if (i==0) 
				logger.warn(pstmt.toString());
			pstmt.executeUpdate();
		}
	    // Do the query.
	    
	    }
	    catch (Exception e) {
	    	e.printStackTrace();
	    	 logger.error(e.toString());
	    	throw new Exception("WriteSpatialPointScenario");
	    }
	    resultsetCount = RESULTSET_COUNT_NA;
	  }

	  /** Clean up resources used by scenario. */
	  public void cleanup() throws Exception
	  {
		 // clean up the inserted records
		 try {  
		   logger.warn(cleanUpPstmt.toString());
		   cleanUpPstmt.executeUpdate();
		   cleanUpPstmt.close();
		}
		catch (Exception e) {
		   	e.printStackTrace();
		    logger.error(e.toString());
		   	throw new Exception("Cleanup WriteSpatialPointScenario");
		}  
		    
	    // Clean up connections. 
	    for (int i = 0; i < pstmtArray.length; i++)
	      pstmtArray[i].close();
	    if (conn != null)
	      conn.close();
	  }
	 
}