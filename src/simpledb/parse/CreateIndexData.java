package simpledb.parse;

import java.util.LinkedList;
import java.util.List;

/**
 * The parser for the <i>create index</i> statement.
 * @author Edward Sciore
 */
public class CreateIndexData {
   private String idxname, tblname, fldname, idxtype;
   
   /**
    * Saves the table and field names of the specified index.
    */
   public CreateIndexData(String idxname, String tblname, String fldname, String idxtype){
      this.idxname = idxname;
      this.tblname = tblname;
      this.fldname = fldname;
      System.out.println("IDX TYPE: " + idxtype);
      if(idxtype.equals("sh") || idxtype.equals("bt") || idxtype.equals("eh")){
         this.idxtype =idxtype;
      }else throw new BadSyntaxException();
   }
   
   /**
    * Returns the name of the index.
    * @return the name of the index
    */
   public String indexName() {
      return idxname;
   }
   
   /**
    * Returns the name of the indexed table.
    * @return the name of the indexed table
    */
   public String tableName() {
      return tblname;
   }
   
   /**
    * Returns the name of the indexed field.
    * @return the name of the indexed field
    */
   public String fieldName() {
      return fldname;
   }
   
   public String indexType() {
	   return idxtype;
   }
}

