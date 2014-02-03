package nl.basjes.hbase.filter;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import nl.basjes.hbase.filter.AlwaysNextColFilter;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.junit.Test;

public class AlwaysNextColFilterTest {
    private static HBaseTestingUtility utility     = null;
    private HTableInterface            table;

    public AlwaysNextColFilterTest() throws Exception {
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
        createTablesAndAddData();
    }

    public final void createTablesAndAddData() throws IOException {
        byte[]        colFamBytes = "F".getBytes();

        table = utility.createTable("FilterTest".getBytes(), colFamBytes);

        Put put = new Put("Row AA".getBytes());
        put.add(colFamBytes, "Col A".getBytes(), "Foo".getBytes());
        table.put(put);

        put = new Put("Row BB".getBytes());
        put.add(colFamBytes, "Col B".getBytes(), "FooFoo".getBytes());
        table.put(put);

        put = new Put("Row CC".getBytes());
        put.add(colFamBytes, "Col C".getBytes(), "Bar".getBytes());
        table.put(put);

        put = new Put("Row DD".getBytes());
        put.add(colFamBytes, "Col D".getBytes(), "BarBar".getBytes());
        table.put(put);
    }

    @Test
    public void runFilterQueryRows() throws Exception {
        Scan s = new Scan();
        FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        flist.addFilter(new PrefixFilter("Row B".getBytes()));
        flist.addFilter(new AlwaysNextColFilter());
        s.setFilter(flist);
        ResultScanner scanner = table.getScanner(s);
        PrintScanner(scanner);
    }

    public static void PrintScanner(ResultScanner scanner) throws IOException {
        long rowcount = 0;
        System.out.println();
        System.out.println("------------------------------------------------------");
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            // print out the row we found and the columns we were looking for
            System.out.println("Rowid: \"" + new String(rr.getRow()) + "\"");
            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> family : rr.getMap().entrySet()) {
                System.out.println("--Family: \"" + new String(family.getKey()) + "\"");
                for (Entry<byte[], NavigableMap<Long, byte[]>> column : family.getValue().entrySet()) {
                    System.out.println("  --Column: " + new String(column.getKey()));
                    for (Entry<Long, byte[]> value : column.getValue().entrySet()) {
                        System.out.println("    --Value (ts=" + value.getKey() + "): \"" + new String(value.getValue()) + "\"");
                    }
                }
            }
            rowcount++;
        }
        if (rowcount == 0) {
            System.out.println("    EMPTY SCANNER    ");
        }
        System.out.println("------------------------------------------------------");
    }

}
