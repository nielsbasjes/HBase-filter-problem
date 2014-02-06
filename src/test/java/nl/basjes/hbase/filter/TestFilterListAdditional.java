package nl.basjes.hbase.filter;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFilterListAdditional {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = null;
  private static HBaseAdmin admin = null;
  private static byte[] tableName = Bytes.toBytes("test");

  /*
   * The result using this prefix filter must be only rows that have a rowid that start
   * with this string.
   */
  @Test
  public void testFilterListPrefixOnly() throws IOException {
    String prefix = "Row C";

    HTable table = new HTable(TestFilterListAdditional.conf, tableName);
    assertTrue("Fail to create the table", admin.tableExists(tableName));

    FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    flist.addFilter(new PrefixFilter(prefix.getBytes()));

    Scan scan = new Scan();
    scan.setFilter(flist);

    ResultScanner scanner = table.getScanner(scan);

    for (Result r: scanner){
      assertTrue("This row does not start with \""+prefix+"\": "+r.toString(),
          Bytes.toStringBinary(r.getRow()).startsWith(prefix));
    }

    table.close();
  }


  /*
   * The result using this 'always next col' filter is an empty result set.
   * No records are in here.
   */
  @Test
  public void testFilterListAlwaysNextColOnly() throws IOException {
    HTable table = new HTable(TestFilterListAdditional.conf, tableName);
    assertTrue("Fail to create the table", admin.tableExists(tableName));

    FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    flist.addFilter(new AlwaysNextColFilter());

    Scan scan = new Scan();
    scan.setFilter(flist);

    ResultScanner scanner = table.getScanner(scan);

    for (Result r: scanner){
      fail("The result set MUST be empty, instead we got "+r.toString());
    }

    table.close();
  }


  /*
   * When we do a "MUST_PASS_ONE" (a logical 'OR') of the above two filters
   * we expect to get the same result as the 'prefix' only result.
   */
  @Test
  public void testFilterListTwoFiltersMustPassOne() throws IOException {
    String prefix = "Row C";

    HTable table = new HTable(TestFilterListAdditional.conf, tableName);
    assertTrue("Fail to create the table", admin.tableExists(tableName));

    FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    flist.addFilter(new PrefixFilter(prefix.getBytes()));
    flist.addFilter(new AlwaysNextColFilter());

    Scan scan = new Scan();
    scan.setFilter(flist);

    ResultScanner scanner = table.getScanner(scan);

    for (Result r: scanner){
      assertTrue("This row does not start with \""+prefix+"\": "+r.toString(),
                 Bytes.toStringBinary(r.getRow()).startsWith(prefix));
    }

    table.close();
  }



  private static void prepareData() {
    try {
      HTable table = new HTable(TestFilterListAdditional.conf, tableName);
      assertTrue("Fail to create the table", admin.tableExists(tableName));

      Put put = new Put("Row AA".getBytes());
      put.add("F".getBytes(), "Col A1".getBytes(), "Foo".getBytes());
      put.add("F".getBytes(), "Col A2".getBytes(), "Foo".getBytes());
      table.put(put);

      put = new Put("Row BB".getBytes());
      put.add("F".getBytes(), "Col B1".getBytes(), "Foo".getBytes());
      put.add("F".getBytes(), "Col B2".getBytes(), "Foo".getBytes());
      table.put(put);

      put = new Put("Row CC".getBytes());
      put.add("F".getBytes(), "Col C1".getBytes(), "Foo".getBytes());
      put.add("F".getBytes(), "Col C2".getBytes(), "Foo".getBytes());
      table.put(put);

      put = new Put("Row DD".getBytes());
      put.add("F".getBytes(), "Col D1".getBytes(), "Foo".getBytes());
      put.add("F".getBytes(), "Col D2".getBytes(), "Foo".getBytes());
      table.put(put);

      table.close();
    } catch (IOException e) {
      assertNull("Exception found while putting data into table", e);
    }
  }

  private static void createTable() {
    assertNotNull("HBaseAdmin is not initialized successfully.", admin);
    if (admin != null) {

      HTableDescriptor desc = new HTableDescriptor(tableName);
      HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("F"));
      desc.addFamily(coldef);

      try {
        admin.createTable(desc);
        assertTrue("Fail to create the table", admin.tableExists(tableName));
      } catch (IOException e) {
        assertNull("Exception found while creating table", e);
      }

    }
  }

  private static void deleteTable() {
    if (admin != null) {
      try {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } catch (IOException e) {
        assertNull("Exception found deleting the table", e);
      }
    }
  }

  private static void initialize(Configuration conf) {
    TestFilterListAdditional.conf = HBaseConfiguration.create(conf);
    TestFilterListAdditional.conf.setInt(
        HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    try {
      admin = new HBaseAdmin(conf);
    } catch (MasterNotRunningException e) {
      assertNull("Master is not running", e);
    } catch (ZooKeeperConnectionException e) {
      assertNull("Cannot connect to Zookeeper", e);
    }
    createTable();
    prepareData();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    initialize(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    deleteTable();
    TEST_UTIL.shutdownMiniCluster();
  }

}
