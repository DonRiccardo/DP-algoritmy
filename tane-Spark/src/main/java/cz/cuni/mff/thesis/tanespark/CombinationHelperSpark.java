/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.thesis.tanespark;

import cz.cuni.mff.thesis.tanespark.model._StrippedPartitionSpark;
import java.util.BitSet;
import java.io.Serializable;

/**
 *
 * @author Richard
 */
public class CombinationHelperSpark implements Serializable{
    private static final long serialVersionUID = 1L;

    private BitSet rhsCandidates;
    private boolean valid;

    private _StrippedPartitionSpark partition;

    public CombinationHelperSpark() {
        valid = true;
    }

    public BitSet getRhsCandidates() {
        return rhsCandidates;
    }

    public void setRhsCandidates(BitSet rhsCandidates) {
        this.rhsCandidates = (BitSet) rhsCandidates.clone();
    }

    public _StrippedPartitionSpark getPartition() {
        return partition;
    }

    public void setPartition(_StrippedPartitionSpark partition) {
        this.partition = partition;
    }

    public boolean isValid() {
        return valid;
    }

    public void setInvalid() {
        this.valid = false;
        partition = null;
    }
}
