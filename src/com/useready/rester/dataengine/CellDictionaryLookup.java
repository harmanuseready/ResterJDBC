// =================================================================================================
///  @file CellDictionaryLookup.java
///
///  Definition of the Class CellDictionaryLookup
///
///  Copyright (C) 2015 Simba Technologies Incorporated
// =================================================================================================

package com.useready.rester.dataengine;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Object that represents a dictionary lookup for a specific cell in an JSON document.
 *
 * Used alone or in a chain with other CellLookups, this object is used for lookups within an
 * object where the key name must be used.
 */
public class CellDictionaryLookup extends CellLookup
{
    /*
     * Instance variable(s) ========================================================================
     */
    
    /**
     * The key name to use in the lookup.
     */
    private String m_key;
    
    /*
     * Constructor(s) ==============================================================================
     */
    
    /**
     * Constructor.
     *
     * @param key
     *          The key name to lookup.
     */
    protected CellDictionaryLookup(String key)
    {
        this.m_key = key;
    }
    
    /*
     * Method(s) ===================================================================================
     */
    
    /**
     * Clone the lookup path.
     *
     * @return A clone of the lookup path.
     */
    protected CellDictionaryLookup clone()
    {
        CellDictionaryLookup clone = new CellDictionaryLookup(m_key);
        
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
        JsonNode subRow = row.findPath(m_key);
        
        if (subRow.isMissingNode())
        {
            return null;
        }
        
        if (null != m_child)
        {
            // There's another section of the lookup, delegate downwards.
            return m_child.getCell(subRow);
        }
        
        return subRow;
    }
}
