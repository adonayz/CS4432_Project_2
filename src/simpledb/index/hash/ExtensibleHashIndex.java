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

import static simpledb.index.hash.ExtensibleHashIndex.Operation.DELETE;
import static simpledb.index.hash.ExtensibleHashIndex.Operation.INSERT;

/**
 * Created by Adonay on 4/26/2018.
 * CS4432-Project2: Whole class is new to the project
 *                  Implement two level indexing
 */
public class ExtensibleHashIndex implements Index{
    enum Operation {
        INSERT ("Insert"),
        DELETE ("Delete");
        String operation;

        Operation(String brand) {
            this.operation = brand;
        }

        @Override
        public String toString(){
            return operation;
        }
    }
    private static String tableName = "ExtensibleTable";
    private Transaction tx;
    private TableScan tableScan;
    private Schema sch = new Schema();
    private InternalHashIndex internalIndex;
    private Constant dataVal;
    private Constant searchKey;
    private RID dataRID;
    private Operation currentOperation = INSERT;
    private static int MAX_SIZE = 1024;

    public ExtensibleHashIndex(String idxname, Schema sch, Transaction tx) {
        this.tx = tx;
        this.sch.addIntField("ext_idx");
        this.sch.addIntField("l_depth");
        internalIndex = new InternalHashIndex(idxname, sch, tx);
    }

    @Override
    public void beforeFirst(Constant searchkey) {
        this.searchKey = searchkey;
        if (currentOperation == INSERT) {
            if(getGDepth() < 0){
                TableInfo tableInfo = new TableInfo(tableName, this.sch);
                tableScan = new TableScan(tableInfo, tx);
                addIndex(tableScan, 0, 0);
            }
        }
        close();

        TableInfo tableInfo = new TableInfo(tableName, this.sch);
        tableScan = new TableScan(tableInfo, tx);
        int bucketKey = getBucketKey(searchKey);

        if(currentOperation == INSERT){
            if(internalIndex.getSize(bucketKey)>= MAX_SIZE){
                tableScan.beforeFirst();
                int localDepth = 0;
                while(tableScan.next()){
                    if(bucketKey == tableScan.getInt("ext_idx")){
                        localDepth = tableScan.getInt("l_depth");
                    }
                }

                if(getGDepth() == localDepth){
                    incrementGDepth();
                }
                bucketKey = splitFullBucket(bucketKey);
            }
        }
        internalIndex.beforeFirst(searchkey, bucketKey);
    }

    @Override
    public boolean next() {
        while(tableScan.next()){
            if(tableScan.getVal("dataval").equals(searchKey)){
                return true;
            }
        }
        return false;
    }

    @Override
    public RID getDataRid() {
        return internalIndex.getDataRid();
    }

    @Override
    public void insert(Constant dataval, RID datarid) {
        currentOperation = INSERT;
        updateLast(dataval, datarid);
        internalIndex.insert(dataval, datarid);
        System.out.println(toString());
    }

    @Override
    public void delete(Constant dataval, RID datarid) {
        currentOperation = DELETE;
        updateLast(dataval, datarid);
        internalIndex.delete(dataval, datarid);
        System.out.println(toString());
    }

    @Override
    public void close() {
        if (tableScan != null){
            tableScan.close();
        }
    }

    public void updateLast(Constant dataval, RID datarid){
        this.dataVal = dataval;
        this.dataRID = datarid;
        beforeFirst(dataval);
    }

    public int splitFullBucket(int bucketKey){
        LinkedList<Constant> initialKeys = new LinkedList<>();
        LinkedList<RID> initialIDs = new LinkedList<>();
        internalIndex.beforeFirst(bucketKey);
        initialKeys = internalIndex.getKeys();
        initialIDs = internalIndex.getRIDs();
        internalIndex.getTableScan().beforeFirst();

        int deletedRecs = 0;
        while (initialIDs.size() > deletedRecs) {
            internalIndex.getTableScan().beforeFirst();
            delete(initialKeys.get(deletedRecs), initialIDs.get(deletedRecs));
            deletedRecs++;
        }

        int newKey = getNewKey(bucketKey);
        int localDepth = getLDepth(bucketKey);

        tableScan.beforeFirst();

        while (tableScan.next()) {
            if (newKey == tableScan.getInt("ext_idx")) {
                tableScan.delete(); // clean index from record file
                break;
            }
        }

        addIndex(tableScan, bucketKey, localDepth + 1); // update local depth

        TableInfo newTable = new TableInfo(tableName, this.sch);
        TableScan newTableScan = new TableScan(newTable, tx);
        addIndex(newTableScan, newKey, localDepth + 1);  // add the new index

        for (int i = 0; i < deletedRecs; i++) {
            insert(initialKeys.get(i), initialIDs.get(i));
        }

        return getBucketKey(searchKey);
    }

    public void addIndex(TableScan tblScn, int bucketKey, int localDepth){
        tblScn.insert();
        tblScn.setInt("ext_idx", bucketKey);
        tblScn.setInt("l_depth", localDepth);
        tblScn.close();
    }

    // returns next index for the inserted record
    public Integer nextIndex(int index) {
        TableInfo tableInfo = new TableInfo(tableName, this.sch);
        TableScan tableScan = new TableScan(tableInfo, tx);
        int localDepth = 0;
        while (tableScan.next()) {
            if (tableScan.getInt("ext_idx") ==index) {
                localDepth = tableScan.getInt("l_depth");
            }
        }
        int next = (int) (index % Math.pow(2, ((double) localDepth)));
        tableScan.close();
        return next;
    }

    public int getNewKey(int key){
        return key + (int) Math.pow(2, getGDepth() - 1);
    }

    public int getLDepth(int bucketKey){
        int localDepth = 0;
        tableScan.beforeFirst();
        while (tableScan.next()) {
            if (tableScan.getInt("ext_idx") == bucketKey) {
                localDepth = tableScan.getInt("l_depth");
                tableScan.delete();
                break;
            }
        }
        return localDepth;
    }


    public Integer getGDepth() {
        TableInfo tableInfo = new TableInfo(tableName, this.sch);
        TableScan tableScan = new TableScan(tableInfo, tx);
        int i = 0;
        while (tableScan.next()) {
            i++;
        }
        int result = (int) Math.ceil((Math.log(i) / Math.log(2)));
        tableScan.close();
        return result;
    }

    public void incrementGDepth(){
        for (int i = 0; i < (int) Math.pow(2, getGDepth()); i++) {
            TableInfo tableInfo = new TableInfo(tableName, this.sch);
            TableScan tableScan = new TableScan(tableInfo, tx);

            int key = (int) Math.pow(2, getGDepth()) + i;
            int globalDepth = 0;
            while (tableScan.next()) {
                if (tableScan.getInt("ext_idx") == i) {
                    globalDepth = tableScan.getInt("l_depth");
                }
            }
            tableScan.close();

            tableInfo = new TableInfo(tableName, this.sch);
            tableScan = new TableScan(tableInfo, tx);
            addIndex(tableScan, key, globalDepth);
        }
    }

    public int getBucketKey(Constant key){
        int bck = Integer.parseInt(key.toString()) % (int) Math.pow(2, getGDepth());
        return nextIndex(bck);
    }

    public String toString(){
        return currentOperation.toString() + ": dataval " + dataVal + "and rid " + dataRID;
    }
}
