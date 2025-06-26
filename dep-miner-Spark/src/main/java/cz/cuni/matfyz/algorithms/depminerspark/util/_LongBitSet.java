/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.matfyz.algorithms.depminerspark.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.lucene.util.OpenBitSet;

/**
 *
 * @author Richard
 */
public class _LongBitSet extends OpenBitSet implements Externalizable{

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(bits);
        out.writeInt(wlen);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bits = (long[]) in.readObject();
        wlen = in.readInt();
    }
    
    
}
