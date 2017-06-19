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
public class DataGeneratorForBit implements DataGenerator
{
  private int length; 
 
  /** Create a new instance with length in bits. */
  DataGeneratorForBit(int length)
  {
    this.length = length;
  }
  
  /** Generate a random value between 0 and the maximum that can be held in the
   * given number of bits
   **/
  public Object generate()
  {
    long max = 1L << (length - 1);
    long retval = (long) (Math.random() * max);  
    return new Long(retval);
  }
}
