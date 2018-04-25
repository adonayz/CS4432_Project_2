package simpledb.record;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import java.util.*;

/**
 * The metadata about a table and its records.
 * @author Edward Sciore
 */
public class TableInfo {
   private Schema schema;
   private Map<String,Integer> offsets;
   private int recordlen;
   private String tblname;
   private boolean isSorted = false; // CS4432-Project2: indicates if tables is sorted or not
   private LinkedList<String> sortedColumns;

   /**
    * Creates a TableInfo object, given a table name
    * and schema. The constructor calculates the
    * physical offset of each field.
    * This constructor is used when a table is created. 
    * @param tblname the name of the table
    * @param schema the schema of the table's records
    */
   public TableInfo(String tblname, Schema schema) {
      this(tblname, schema, new HashMap<>(), 0, false, new LinkedList<>());
      int pos = 0;
      for (String fldname : schema.fields()) {
         this.offsets.put(fldname, pos);
         pos += lengthInBytes(fldname);
      }
      this.recordlen = pos;
   }
   
   /**
    * Creates a TableInfo object from the 
    * specified metadata.
    * This constructor is used when the metadata
    * is retrieved from the catalog.
    * @param tblname the name of the table
    * @param schema the schema of the table's records
    * @param offsets the already-calculated offsets of the fields within a record
    * @param recordlen the already-calculated length of each record
    */
   public TableInfo(String tblname, Schema schema, Map<String,Integer> offsets, int recordlen) {
      this(tblname, schema, new HashMap<>(), recordlen, false, new LinkedList<>());
   }

   // CS4432-Project2: The following constructor was added for this project
   /**
    * Creates a TableInfo object from the
    * specified metadata.
    * This constructor is used when the metadata
    * is retrieved from the catalog.
    * @param tblname the name of the table
    * @param schema the schema of the table's records
    * @param offsets the already-calculated offsets of the fields within a record
    * @param recordlen the already-calculated length of each record
    * @param isSorted indicates if records have been sorted
    * @param sortedColumns
    */
   public TableInfo(String tblname, Schema schema, Map<String,Integer> offsets, int recordlen, boolean isSorted, LinkedList<String> sortedColumns) {
      this.tblname   = tblname;
      this.schema    = schema;
      this.offsets   = offsets;
      this.recordlen = recordlen;
      this.isSorted  = isSorted;
      this.sortedColumns   = sortedColumns ;
   }
   
   /**
    * Returns the filename assigned to this table.
    * Currently, the filename is the table name
    * followed by ".tbl".
    * @return the name of the file assigned to the table
    */
   public String fileName() {
      return tblname + ".tbl";
   }
   
   /**
    * Returns the schema of the table's records
    * @return the table's record schema
    */
   public Schema schema() {
      return schema;
   }
   
   /**
    * Returns the offset of a specified field within a record
    * @param fldname the name of the field
    * @return the offset of that field within a record
    */
   public int offset(String fldname) {
      return offsets.get(fldname);
   }
   
   /**
    * Returns the length of a record, in bytes.
    * @return the length in bytes of a record
    */
   public int recordLength() {
      return recordlen;
   }
   
   private int lengthInBytes(String fldname) {
      int fldtype = schema.type(fldname);
      if (fldtype == INTEGER)
         return INT_SIZE;
      else
         return STR_SIZE(schema.length(fldname));
   }

   // CS4432-Project2: getter for isSorted variable
   public boolean isSorted() {
      return isSorted;
   }

   // CS4432-Project2: setter for isSorted variable
   public void setSorted(boolean sorted) {
      isSorted = sorted;
   }

   public LinkedList<String> getSortedColumns() {
      return sortedColumns;
   }

   public void setSortedColumns(LinkedList<String> sortedColumns) {
      this.sortedColumns = sortedColumns;
   }
}