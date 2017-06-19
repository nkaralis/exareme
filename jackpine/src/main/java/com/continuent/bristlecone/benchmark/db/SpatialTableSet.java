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

import java.util.HashMap;
import java.util.List;

import com.continuent.bristlecone.utils.ToStringHelper;


/**
 * Describes the content of a set of spatial tables
 * 
 * @author sray
 *
 */
public class SpatialTableSet
{
  
  private final int count;
  private HashMap tables;
  private List<DataGenerator> dataGenerators;
  
  public SpatialTableSet(int numTables) {
	  tables = new HashMap(); 
	  count = numTables;
  }
  
  public void addTable(String name, int id, Column[] columns)
  {
	  Table t = new Table(name, columns);
	  tables.put(id,t);
   
  }

  /** Returns the table column definitions. */
  public Column[] getColumns(String name)
  {
    return ((Table)tables.get(name)).getColumns();
  }

  /** Returns the number of tables in the group. */
  public int getCount()
  {
    return count;
  }

  
  
  /** Returns a list of table definitions. */
  public synchronized Table[] getTables()
  {
	Table[] spatialTables = new Table[count]; 
	int i=0;
    if (tables != null && i< count)
    {
      for (Object key : tables.keySet()) {
        Table t = (Table)tables.get(key);
        spatialTables[i]=t;
        i++;
      }
    }
    return spatialTables;
  }
  
  /** Returns a random list of tables.  This is handy for join tests. */
  public Table[] getRandomTables(int howMany)
  {
	Table[] randomTables = null;
	/*  
    // Ensure the number of tables requested is not more than we have. 
    getTables();
    if (howMany > tables.length)
      throw new Error("Caller requested too many random tables: requested=" 
          + howMany + " available=" + tables.length);
    
    // Generate index of the first table and start reading tables from 
    // that point.  
    randomTables = new Table[howMany];
    int index = (int) Math.random() * tables.length;
    for (int i = 0; i < howMany; i++)
    {
      randomTables[i] = tables[index++];
      if (index > tables.length)
        index = 0;
    }
    */
    return randomTables;
  }

  
  public synchronized List<DataGenerator> getDataGenerators()
  {
	 /*
    if (dataGenerators == null)
    {
      // Set up column generators for data. 
      Column[] columns = getColumns();
      dataGenerators = new ArrayList<DataGenerator>();
      
      for (int i = 0; i < columns.length; i++)
      {
        if (! columns[i].isAutoIncrement())
        {
          dataGenerators.add(DataGeneratorFactory.getInstance().getGenerator(columns[i]));
        }
      }
    }
    */
    return dataGenerators; 
  }
  
  @Override public String toString()
  {
      return ToStringHelper.toString(this);
  }
}
