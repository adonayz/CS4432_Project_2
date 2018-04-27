package simpledb.index.hash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Adonay on 4/26/2018.
 */
public class InternalHashIndex implements Index {
    private String idxname;
    private Schema sch;
    private Transaction tx;
    private Constant searchkey = null;
    private TableScan tableScan = null;

    public InternalHashIndex(String idxname, Schema sch, Transaction tx) {
        this.idxname = idxname;
        this.sch = sch;
        this.tx = tx;
    }

    public void beforeFirst(Constant searchkey) {

    }

    public void beforeFirst(Constant searchkey, int bufferKey) {
        close();
        this.searchkey = searchkey;
        String tblname = idxname + bufferKey;
        TableInfo ti = new TableInfo(tblname, sch);
        tableScan = new TableScan(ti, tx);
    }

    public void beforeFirst(int key) {
        close();
        String tblname = idxname + key;
        TableInfo ti = new TableInfo(tblname, sch);
        tableScan = new TableScan(ti, tx);
    }

    @Override
    public boolean next() {
        while (tableScan.next()) {
            if (tableScan.getVal("dataval").equals(searchkey))
                return true;
        }
        return false;
    }

    @Override
    public RID getDataRid() {
        int blknum = tableScan.getInt("block");
        int id = tableScan.getInt("id");
        return new RID(blknum, id);
    }

    @Override
    public void insert(Constant dataval, RID datarid) {tableScan.insert();
        tableScan.setInt("block", datarid.blockNumber());
        tableScan.setInt("id", datarid.id());
        tableScan.setVal("dataval", dataval);
    }

    @Override
    public void delete(Constant dataval, RID datarid) {
        while(next())
            if (getDataRid().equals(datarid)) {
                tableScan.delete();
                return;
            }
    }

    @Override
    public void close() {
        if (tableScan != null)
            tableScan.close();
    }

    public int getSize(int key) {
        String tblname = idxname + key;
        TableInfo ti2 = new TableInfo(tblname, sch);
        TableScan ts2 = new TableScan(ti2, tx);

        int n = 0;
        while (ts2.next()) {
            n++;
        }
        return n;
    }

    public LinkedList<Constant> getKeys() {
        LinkedList<Constant> keys= new LinkedList<Constant>();
        tableScan.beforeFirst();
        while(tableScan.next()) {
            keys.add(tableScan.getVal("dataval"));
        }
        return keys;
    }

    public LinkedList<RID> getRIDs() {
        LinkedList<RID> rids= new LinkedList<RID>();
        tableScan.beforeFirst();
        while(tableScan.next()) {
            rids.add(getDataRid());
        }
        return rids;
    }

    public TableScan getTableScan() {
        return tableScan;
    }
}
