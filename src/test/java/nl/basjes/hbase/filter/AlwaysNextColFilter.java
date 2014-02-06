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
        return ReturnCode.NEXT_COL;
    }

    public void write(DataOutput out) throws IOException { }

    public void readFields(DataInput in) throws IOException { }
}
