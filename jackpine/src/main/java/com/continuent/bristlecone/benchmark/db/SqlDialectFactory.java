/**
 * Jackpine Spatial Database Benchmark 
 *  Copyright (C) 2010 University of Toronto
 * 
 * Jackpine is based on Bristlecone Tool for Databases
 * Wherever applicable: Copyright (C) 2006-2007 Continuent Inc.
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
 * Initial developer(s): Robert Hodges and Ralph Hannus.
 * Contributor(s): 
 */

package com.continuent.bristlecone.benchmark.db;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * @author rhodges
 *
 */
public class SqlDialectFactory
{
  private static final Logger logger = Logger.getLogger(SqlDialectFactory.class);
  private static final SqlDialectFactory instance = new SqlDialectFactory();
  
  // Declare and initialized dialect support. 
  private static final List<SqlDialect> dialects;
  static
  {
    List<SqlDialect> al = new ArrayList<SqlDialect>();
  

    al.add(new SqlDialectForDerby());
    al.add(new SqlDialectForHSQLDB());
    al.add(new SqlDialectForInformix());
    al.add(new SqlDialectForIngres());
    al.add(new SqlDialectForMysql());
    al.add(new SqlDialectForOracle());
    al.add(new SqlDialectForPostgreSQL());
    al.add(new SqlDialectForPCluster());
    al.add(new SqlDialectForMCluster());
    al.add(new SqlDialectForExareme());
    dialects = al;
  }
 

  /** 
   * Returns factory instance. 
   */
  public static SqlDialectFactory getInstance()
  {
    return instance;
  }

  /**
   * Return a dialect that processes the given JDBC URL or null if no match
   * can be found. 
   * 
   * @param url A JDBC URL
   */
  public SqlDialect getDialect(String url)
  {
	//System.out.println("*********** Inside getDialect()");  
    int count = dialects.size();
    for (int i = 0; i < count; i++)
    {
      //System.out.println(dialects.get(i).getClass());	
      if (dialects.get(i).supportsJdbcUrl(url)) {
    	String className = dialects.get(i).getClass().toString();  
    	className = className.substring(className.lastIndexOf(".SqlDialectFor")+1) ;
    	System.out.println("\nRunning scenario for "+ className);	  
    	logger.warn("\nRunning scenario for "+ className);
    	return dialects.get(i);
      }
    }
    return null;
  }
}