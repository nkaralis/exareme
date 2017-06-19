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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import edu.toronto.cs.jackpine.benchmark.db.SpatialSqlDialect;

/**  
 * 
 * @author sray
 */
public class ReadSpatialLongestLine extends SpatialScenarioBase
{
  private static final Logger logger = Logger.getLogger(ReadSpatialLongestLine.class);
  
  /** exareme does not support prepared statement -> we have to use simple statement */
  protected PreparedStatement[] pstmtArray;
  protected Statement stmt;
  protected String sql;
  
  
  /** Create a prepared statement array. */
  public void prepare() throws Exception
  {
	  SpatialSqlDialect dialect = helper.getSpatialSqlDialect(); 
	    
	    //pstmtArray = new PreparedStatement[1];
	    stmt = conn.createStatement();
	    sql = dialect.getSelectLongestLine();
	    //pstmtArray[0] = conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	    //stmt.executeQuery("SELECT * FROM lilou4");
  }

  /** Execute an interation. */
  public void iterate(long iterationCount) throws Exception
  {
    //PreparedStatement pstmt = pstmtArray[0];
	Statement stmt = this.stmt;
    
    // Do the query.
    logger.warn(stmt.toString());
    ResultSet r = stmt.executeQuery("SELECT * FROM lilou4 limit1");
    resultsetCount = RESULTSET_COUNT_NA; 
    if (r.next()) {
    	resultsetCount = r.getLong(1);
    } 
  }

  /** Clean up resources used by scenario. */
  public void cleanup() throws Exception
  {
    // Clean up connections. 
    //for (int i = 0; i < pstmtArray.length; i++)
    //  pstmtArray[i].close();
    stmt.close();
	if (conn != null)
      conn.close();
  }
  
  public static void printResultset(ResultSet rs) throws SQLException {
      logger.info("Columns: " + rs.getMetaData().getColumnCount());
      int count = 0;
      int size = 0;
      for (int c = 0; c < rs.getMetaData().getColumnCount(); ++c) {
          logger.info(rs.getMetaData().getColumnName(c + 1));
      }
      while (rs.next()) {
          String[] next = new String[rs.getMetaData().getColumnCount()];
          for (int c = 0; c < next.length; ++c) {
              next[c] = "" + rs.getObject(c + 1);
              size += next[c].length();
          }
          for (String v : next) {
              logger.info(v + "\t");
          }
          logger.info("");
          ++count;
      }
      System.out.println("Count: " + count + "\n\tSize: " + size);
  }
 
}
