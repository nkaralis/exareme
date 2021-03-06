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
 * Returns a data generator appropriate for a particular data type. 
 * 
 *
 */
public class DataGeneratorFactory
{
  private static DataGeneratorFactory instance = new DataGeneratorFactory();

  // Not used outside this class. 
  private DataGeneratorFactory()
  {
  }

  /** 
   * Returns factory instance. 
   */
  public static DataGeneratorFactory getInstance()
  {
    return instance;
  }
  
  /**
   * Return a data type generator for a particular column type.  
   */
  public DataGenerator getGenerator(Column c)
  {
    switch (c.getType())
    {
      case java.sql.Types.BIT: 
        return new DataGeneratorForBit(c.getLength());
      case java.sql.Types.BLOB: 
        return new DataGeneratorForBlob(c.getLength(), 10);
      case java.sql.Types.CHAR:
        return new DataGeneratorForString(c.getLength(), 10);
      case java.sql.Types.CLOB:
          return new DataGeneratorForString(c.getLength(), 10);
      case AdditionalTypes.XML:
          return new DataGeneratorForXML(c.getLength(), 10);
      case java.sql.Types.DOUBLE:
        return new DataGeneratorForDouble();
      case java.sql.Types.FLOAT:
        return new DataGeneratorForFloat();
      case java.sql.Types.INTEGER:
          return new DataGeneratorForLong(Integer.MAX_VALUE);
      case java.sql.Types.DECIMAL:
          return new DataGeneratorForDecimal(c.getLength(), c.getPrecision());
      case java.sql.Types.TINYINT:
        return new DataGeneratorForLong(127); 
      case java.sql.Types.SMALLINT:
        return new DataGeneratorForLong(32767); 
      case AdditionalTypes.MEDIUMINT:
        return new DataGeneratorForLong(8388607); 
      case java.sql.Types.BIGINT:
        return new DataGeneratorForLong(Long.MAX_VALUE); 
      case AdditionalTypes.UTINYINT:
        return new DataGeneratorForUBigInt("255"); 
      case AdditionalTypes.USMALLINT:
        return new DataGeneratorForUBigInt("65535"); 
      case AdditionalTypes.UMEDIUMINT:
        return new DataGeneratorForUBigInt("16777215"); 
      case AdditionalTypes.UINT:
        return new DataGeneratorForUBigInt("4294967295"); 
      case AdditionalTypes.UBIGINT:
        return new DataGeneratorForUBigInt("18446744073709551615"); 
      case java.sql.Types.VARCHAR:
        return new DataGeneratorForString(c.getLength(), 10);
      case java.sql.Types.BOOLEAN:
        return new DataGeneratorForBoolean();
      case java.sql.Types.DATE:
        return new DataGeneratorForDate();
      case java.sql.Types.TIME:
        return new DataGeneratorForTime();
      case AdditionalTypes.TIMESTAMPLOCAL :
      case java.sql.Types.TIMESTAMP:
        return new DataGeneratorForTimestamp();
      
      default:
        throw new IllegalArgumentException("Unsupported JDBC type value: " + c.getType());
    }
  }
}
