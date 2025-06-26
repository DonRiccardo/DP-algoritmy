package cz.cuni.mff.algorithms.fastfds_spark.model;

import java.util.BitSet;

import cz.cuni.mff.algorithms.fastfds_spark.model._AgreeSet;
import cz.cuni.mff.algorithms.fastfds_spark.util._BitSetUtil;

public class _DifferenceSet extends _AgreeSet {

    public _DifferenceSet(BitSet obs) {

        this.attributes = obs;
    }

    public _DifferenceSet() {

        this(new BitSet());
    }
    
    @Override
    public String toString() {

        //return "diff(" + _BitSetUtil.convertToIntList(this.attributes).toString() + ")";
        return "diff("+this.attributes+")";
    }
}
