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
 * Generates integer values. 
 * 
 *
 */
public class DataGeneratorForLong implements DataGenerator
{
  private long max; 
 
  /** Create a new instance with an upper bound. */
  DataGeneratorForLong(long maxValue)
  {
    this.max = maxValue;
  }
  
  /** Generate next value up to the boundary value. */
  public Object generate()
  {
    long sign = (Math.random() >= 0.5) ? -1 : 1;
    long absvalue = (long) (Math.random() * max);  
    return new Long(sign * absvalue);
  }
}
