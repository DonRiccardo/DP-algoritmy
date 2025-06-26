package cz.cuni.mff.algorithms.fastfds_spark.model;

import java.util.BitSet;

import cz.cuni.mff.algorithms.fastfds_spark.util._BitSetUtil;
import java.io.Serializable;

public class _AgreeSet implements Serializable{//extends StorageSet {

    protected BitSet attributes = new BitSet();

    public void add(int attribute) {

        this.attributes.set(attribute);
    }
    
    public BitSet getAttributes() {

        return (BitSet) this.attributes.clone();

    }

    @Override
    public String toString() {

        return "ag(" + _BitSetUtil.convertToIntList(this.attributes).toString()
                + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((attributes == null) ? 0 : attributes.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        _AgreeSet other = (_AgreeSet) obj;
        if (attributes == null) {
            if (other.attributes != null)
                return false;
        } else if (!attributes.equals(other.attributes))
            return false;
        return true;
    }

}
