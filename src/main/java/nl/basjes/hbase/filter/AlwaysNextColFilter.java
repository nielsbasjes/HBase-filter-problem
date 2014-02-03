package nl.basjes.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;

public class AlwaysNextColFilter extends FilterBase {
    public AlwaysNextColFilter() {
        super();
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue v) {
//        System.out.println(createDebugLogLine(v, ReturnCode.NEXT_COL));
        return ReturnCode.NEXT_COL;
    }

//    private String createDebugLogLine(KeyValue v, ReturnCode returncode){
//        return toString() + ": ("+v.getTimestamp()+")" + Bytes.toStringBinary(v.getRow(), 0, v.getRow().length) + " (=\"" + Bytes.toStringBinary(v.getValue(), 0, v.getValue().length) + "\") --> " + returncode.toString();
//    }

//    @Override
//    public String toString() {
//        return this.getClass().getSimpleName();
//    }

    public void write(DataOutput out) throws IOException { }

    public void readFields(DataInput in) throws IOException { }
}
