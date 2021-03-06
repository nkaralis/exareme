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

/**
 * MySQL DBMS dialect information.
 * 
 * @author rhodges
 */
public class SqlDialectForMysql extends AbstractSqlDialect
{
  public SqlDialectForMysql() {
  }
	
  /** Return MySQL driver. */
  public String getDriver()
  {
    return "com.mysql.jdbc.Driver";
  }

  /** Returns true if the JDBC URL looks like a PostgreSQL URL. */
  public boolean supportsJdbcUrl(String url)
  {
    return (url.startsWith("jdbc:mysql"));
  }

  /**
   * Implements create table that supports defining database engine.
   */
  public String getCreateTable(Table t)
  {
    String sql = super.getCreateTable(t);
    if (t.getDatabaseEngine() != null)
      sql += " ENGINE= " + t.getDatabaseEngine();

    return sql;
  }

  /**
   * Transforms negative fetch sizes to Integer.MIN_VALUE, which prompts
   * row-by-row streaming of result sets.
   */
  public int implementationConvertFetchSize(int fetchSize)
  {
    if (fetchSize < 0)
      return Integer.MIN_VALUE;
    else
      return fetchSize;
  }

  /** Add support for specialized MySQL BLOB/CLOB names. */
  public String implementationTypeName(int type)
  {
    switch (type)
    {
      case java.sql.Types.BLOB :
        return "longblob";
      case java.sql.Types.CLOB :
        return "longtext";
      case java.sql.Types.BIGINT :
        return "bigint";
      case AdditionalTypes.UTINYINT :
        return "tinyint unsigned";
      case AdditionalTypes.USMALLINT :
        return "smallint unsigned";
      case AdditionalTypes.UMEDIUMINT :
        return "mediumint unsigned";
      case AdditionalTypes.UINT :
        return "int unsigned";
      case AdditionalTypes.UBIGINT :
        return "bigint unsigned";
      case AdditionalTypes.MEDIUMINT :
        return "mediumint";
      default :
        return super.implementationTypeName(type);
    }
  }
  
}
