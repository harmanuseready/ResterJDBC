// =================================================================================================
///  @file CellIndexLookup.java
///
///  Definition of the Class CellIndexLookup
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.dataengine;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Object that represents an array index lookup for a specific cell in an JSON document.
 *
 * Used alone or in a chain with other CellLookups, this object is used for lookups within an array.
 */
public class CellIndexLookup extends CellLookup
{
    /*
     * Instance variable(s) ========================================================================
     */
    
    /**
     * The 0-based index to use in the lookup.
     */
    private int m_index;
    
    /*
     * Constructor(s) ==============================================================================
     */
    
    /**
     * Constructor.
     *
     * @param index
     *          The array index to lookup.
     */
    protected CellIndexLookup(int index)
    {
        m_index = index;
    }
    
    /*
     * Method(s) ===================================================================================
     */
    
    /**
     * Clone the lookup path.
     *
     * @return A clone of the lookup path.
     */
    protected CellIndexLookup clone()
    {
        CellIndexLookup clone = new CellIndexLookup(m_index);
        
        if (null != m_child)
        {
            clone.addChild(m_child.clone());
        }
        
        return clone;
    }
    
    /**
     * Perform the lookup to get the cell represented by this object.
     *
     * @param row
     *          The json object representing the current row.
     *
     * @return The json object representing the cell at the end of the lookup.
     */
    protected JsonNode getCell(JsonNode row)
    {
        if (row.size() <= m_index)
        {
            return null;
        }
        else
        {
            JsonNode subRow = row.get(m_index);
            
            if (null != m_child)
            {
                // There's another section of the lookup, delegate downwards.
                return m_child.getCell(subRow);
            }
            
            return subRow;
        }
    }
}
