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

package com.continuent.bristlecone.utils;

import java.lang.reflect.Field;

public class ToStringHelper
{

    static public String toString(Object obj)
    {
        StringBuilder result = new StringBuilder();
        String newLine = System.getProperty("line.separator");

        //result.append(obj.getClass().getName());
        result.append("{");
        result.append(newLine);

        // determine fields declared in this class only (no fields of
        // superclass)
        Field[] fields = obj.getClass().getDeclaredFields();

        // print field names paired with their values
        for (Field field : fields)
        {
            field.setAccessible(true);
            try
            {
                result.append(field.getName());
                result.append("=");
                result.append(field.get(obj));
            }
            catch (IllegalAccessException ex)
            {
                System.out.println(ex);
            }
            result.append(newLine);
        }
        result.append("}");

        return result.toString();
    }
}
