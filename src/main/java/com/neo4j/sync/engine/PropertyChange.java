package com.neo4j.sync.engine;

/**
 * com.neo4j.sync.engine.PropertyChange class is used to track before and after value changes
 * that are written to the transaction data event object passed to us through the transaction
 * event listener.  This is then used by the com.neo4j.sync.engine.TransactionRecorder
 *
 * @author Ravi Anthapu
 */

public class PropertyChange {

    private String propertyName;
    private Object oldValue;
    private Object newValue;

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public Object getOldValue() {
        return oldValue;
    }

    public void setOldValue(Object oldValue) {
        this.oldValue = oldValue;
    }

    public Object getNewValue() {
        return newValue;
    }

    public void setNewValue(Object newValue) {
        this.newValue = newValue;
    }
}
