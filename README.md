HBase-filter-problem
====================

I'm trying to write a custom filter that is to be used (in my use case) in a FilterList.
Because I ran into something I do not understand I reduced the code to the absolute minimal and posted it here:
https://github.com/nielsbasjes/HBase-filter-problem

What I now have (just to show my problem) is a filter (classname = AlwaysNextColFilter) that only implements 

    @Override
    public ReturnCode filterKeyValue(KeyValue v) {
        return ReturnCode.NEXT_COL;
    }

The expected behavior is that this filter should indicate to everything that is offered that it should be filtered out.

I then:
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();

and I put some rows in there.

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

Now I create a scan (to scan the entire table ... of 4 rows) and I set the filters.

1) With this I get an empty result set.  (= Good/As I expect it)
        s.setFilter(new AlwaysNextColFilter());

2) With this I get an empty result set.  (= Good/As I expect it)
        FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        flist.addFilter(new AlwaysNextColFilter());
        s.setFilter(flist);

3) With this I get only the rows starting with "Row B".  (= Good/As I expect it)
        FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        flist.addFilter(new PrefixFilter("Row B".getBytes()));
        s.setFilter(flist);

4) With this I get only the rows starting with "Row A" and "Row B".  (= NOT as I expect it)
        FilterList flist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        flist.addFilter(new AlwaysNextColFilter());
        flist.addFilter(new PrefixFilter("Row B".getBytes()));
        s.setFilter(flist);

In 4) I expected to get ONLY the rows starting with "Row B" because these are the only ones that "PASS" at least one of the provided filters.

Did I misunderstand the way this should work ... or is this bug ( in FilterList? )?

