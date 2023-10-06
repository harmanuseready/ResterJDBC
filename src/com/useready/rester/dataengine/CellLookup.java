// =================================================================================================
///  @file CellLookup.java
///
///  Definition of the Class CellLookup
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.dataengine;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Abstract class that represents a lookup path for a specific cell in an JSON document.
 *
 * Lookups will occur via the key name (a dictionary lookup), or array index (index lookup).
 * Lookups may consist of multiple chained lookups, for instance a nested array would require
 * a dictionary lookup (to get the array) and an array lookup (to get the specific array element).
 */
public abstract class CellLookup implements Cloneable
{
    /*
     * Instance variable(s) ========================================================================
     */
    
    /**
     * The next lookup in the path.
     */
    protected CellLookup m_child = null;
    
    /*
     * Method(s) ===================================================================================
     */
    
    /**
     * Add the child lookup in the lookup path.
     *
     * @param cell
     *          The child lookup in the lookup path.
     */
    protected void addChild(CellLookup cell)
    {
        if (null != m_child)
        {
            m_child.addChild(cell);
        }
        else
        {
            m_child = cell;
        }
    }
    
    /**
     * Clone the lookup path.
     *
     * @return A clone of the lookup path.
     */
    abstract protected CellLookup clone();
    
    /**
     * Perform the lookup to get the cell represented by this object.
     *
     * @param row
     *          The json object representing the current row.
     *
     * @return The json object representing the cell at the end of the lookup.
     */
    abstract protected JsonNode getCell(JsonNode row);
}
