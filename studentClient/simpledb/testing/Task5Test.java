package testing;

import simpledb.remote.SimpleDriver;

import java.sql.*;
import java.util.LinkedList;
import java.util.Random;

/**
 * Created by Adonay on 4/25/2018.
 */
public class Task5Test {
    private static int MAX_RECORDS = 10000;
    public static void main(String[] args) {
        Connection conn = null;
        try {
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            // CREATING TWO TABLES. TABLE LHS AND TABLE RHS

            stmt.executeUpdate("CREATE TABLE LHS (a int, b int)");
            System.out.println("Table LHS created");

            stmt.executeUpdate("CREATE TABLE RHS (c int, d int)");
            System.out.println("Table RHS created");

            Random rand =  new Random(1);
            for (int j = 0; j < MAX_RECORDS; j++) {
                System.out.println("inserting record #" + j + " into table LHS");
                String query = "INSERT INTO LHS(a, b) VALUES ("
                        + rand.nextInt(MAX_RECORDS) + "," + rand.nextInt(MAX_RECORDS) + ", 'void' )";
                stmt.executeUpdate(query);
            }
            System.out.println(String.valueOf(MAX_RECORDS) + " records inserted into LHS table\n");

            rand = new Random(1);
            for (int j = 0; j < MAX_RECORDS; j++) {
                System.out.println("inserting record #" + j + " into table RHS");
                String query = "INSERT INTO RHS(c, d) VALUES ("
                        + rand.nextInt(MAX_RECORDS) + "," + rand.nextInt(MAX_RECORDS) + ", 'void' )";
                stmt.executeUpdate(query);
            }
            System.out.println(String.valueOf(MAX_RECORDS) + " records inserted into RHS table\n");

            // TESTING THE JOIN QUERY TWICE TO SEE THE DIFFERENCE IN TIME
            LinkedList<Long> elapsed_times = new LinkedList<>();
            int counter = 2;
            while(counter !=0){
                System.out.println("Testing Join Query. Attempt No. : " + (3-counter));
                ResultSet rslt;
                long start,end;
                String query = "SELECT a, b, c, d FROM LHS, RHS WHERE a = c";
                System.out.println("Query: "+ query + "\ntesting...\n");
                start = System.currentTimeMillis();
                rslt = stmt.executeQuery(query);
                while (rslt.next()) {
                    System.out.println("a : " + rslt.getInt("a") + " c : "
                            + rslt.getInt("c") + " b : " + rslt.getInt("b") +" d : " + rslt.getInt("d"));
                }
                end = System.currentTimeMillis();
                elapsed_times.add(end-start);
                System.out.println("\n Duration of join attempt: " + (end - start) + " ms\n");
                System.out.println("-----------------------------------------------------\n\n");
                counter--;
            }

            System.out.println("FINAL REPORT:");
            System.out.println("Number of records per table: " + String.valueOf(MAX_RECORDS));
            for(int k = 0; k < elapsed_times.size(); k++){
                System.out.println("Attempt " + String.valueOf(k+1) + ": " + String.valueOf(elapsed_times.get(k)) + " ms");
            }


        }catch(SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                if (conn != null)
                    conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
