package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.List;

/**
 * Created by Adonay on 4/25/2018.
 * CS4432-Project2: This class was created for this specific class project
 * It overrides most methods in SortScan inorder to efficiently avoid
 * sorting tables that have already been sorted.
 */
public class ExtendedSortScan extends SortScan {
    private TablePlan tablePlan;
    private UpdateScan mainTable;
    private boolean isSorted;
    private Transaction transaction;


    /**
     * Creates a sort scan, given a list of 1 or 2 runs.
     * If there is only 1 run, then s2 will be null and
     * hasmore2 will be false.
     *  @param runs the list of runs
     * @param comp the record comparator
     * @param plan
     * @param transaction
     */
    public ExtendedSortScan(List<TempTable> runs, RecordComparator comp, Plan plan, Transaction transaction) {
        super(runs, comp);
        tablePlan = (TablePlan) plan;
        this.transaction = transaction;
        this.mainTable = (UpdateScan) tablePlan.open();
        this.isSorted = this.tablePlan.getTableInfo().isSorted();
    }

    @Override
    public void beforeFirst() {
        if(isSorted){
            mainTable.beforeFirst();
        }else{
            super.beforeFirst();
        }
    }

    @Override
    public boolean next() {
        boolean mainHasNext = mainTable.next();
        // if its not sorted
        if(!tablePlan.getTableInfo().isSorted()){
            boolean currentHasNext = super.next();

            if(currentHasNext){
                if(!mainHasNext){
                    mainTable.insert();
                }
                for(String name : tablePlan.getTableInfo().schema().fields()){
                    mainTable.setVal(name, super.getVal(name));
                }
            }else{
                tablePlan.getTableInfo().setSorted(true);
                SimpleDB.mdMgr().broadcastSort(tablePlan.getTableInfo(), transaction);
                System.out.println("TESTING!!");
            }
            return currentHasNext;
        }else{
            // if it is sorted just return the main table's status
            return mainHasNext;
        }
    }

    @Override
    public void close() {
        super.close();
        mainTable.close();
    }

    @Override
    public Constant getVal(String fldname) {
        if(isSorted){
            return mainTable.getVal(fldname);
        }else{
            return super.getVal(fldname);
        }
    }

    @Override
    public int getInt(String fldname) {
        if(isSorted){
            return mainTable.getInt(fldname);
        }else{
            return super.getInt(fldname);
        }
    }

    @Override
    public String getString(String fldname) {
        if(isSorted){
            return mainTable.getString(fldname);
        }else{
            return super.getString(fldname);
        }
    }

    @Override
    public boolean hasField(String fldname) {
        if(isSorted){
            return mainTable.hasField(fldname);
        }else{
            return super.hasField(fldname);
        }
    }

    @Override
    public void savePosition() {
        if(!isSorted){
            super.savePosition();
        }
    }

    @Override
    public void restorePosition() {
        if(!isSorted) {
            super.restorePosition();
        }
    }

    public boolean isSorted() {
        return isSorted;
    }

    public void setSorted(boolean sorted) {
        isSorted = sorted;
    }
}
