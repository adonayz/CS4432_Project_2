package testing;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import simpledb.remote.SimpleDriver;

public class Task3Test {
    final static int maxSize = 1000;

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Connection conn = null;
        Driver d = new SimpleDriver();
        String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
        String url = "jdbc:simpledb://" + host;
        String qry = "Create table test1" +
                "( a1 int," +
                "  a2 int" +
                ")";
        Random rand = null;
        Statement s = null;
        try {
            conn = d.connect(url, null);
            s = conn.createStatement();
            s.executeUpdate("Create table test1" +
                    "( a1 int," +
                    "  a2 int" +
                    ")");
            s.executeUpdate("Create table test2" +
                    "( a1 int," +
                    "  a2 int" +
                    ")");
            s.executeUpdate("Create table test3" +
                    "( a1 int," +
                    "  a2 int" +
                    ")");
            s.executeUpdate("Create table test4" +
                    "( a1 int," +
                    "  a2 int" +
                    ")");
            s.executeUpdate("Create table test5" +
                    "( a3 int," +
                    "  a4 int" +
                    ")");
            System.out.println("Tables Created!");

            s.executeUpdate("create sh index idx1 on test1 (a1)");
            s.executeUpdate("create eh index idx2 on test2 (a1)");
            s.executeUpdate("create bt index idx3 on test3 (a1)");
            System.out.println("Indexes Created!\n");

            for (int i = 1; i < 6; i++) {
                System.out.println("Inserting values to Table test" + i + "...");
                if (i != 5) {
                    rand = new Random(1);// ensure every table gets the same data
                    for (int j = 0; j < maxSize; j++) {
                        s.executeUpdate("insert into test" + i + " (a1,a2) values(" + rand.nextInt(100) + "," + rand.nextInt(100) + ")");
                    }
                } else//case where i=5
                {
                    for (int j = 0; j < maxSize / 2; j++)// insert 10000 records into test5
                    {
                        s.executeUpdate("insert into test" + i + " (a3,a4) values(" + j + "," + j + ")");
                    }
                }
                System.out.println("Finished inserting into Table test" + i + "\n");
            }

            long start,end;

            // Select tests
            System.out.println("Task 3: SELECT TESTS\n");
            start = System.currentTimeMillis();
            s.executeQuery("SELECT a1,a2 FROM test1 where a1=" + 50 + ";");
            end = System.currentTimeMillis();
            System.out.println("Query: " + "SELECT a1,a2 FROM test1 where a1=" + 50 + ";");
            System.out.println("Elapsed Time: " + (end - start)  + " ms\n");

            start = System.currentTimeMillis();
            s.executeQuery("SELECT a1,a2 FROM test2 where a1=" + 50 + ";");
            end = System.currentTimeMillis();
            System.out.println("Query: " + "SELECT a1,a2 FROM test2 where a1=" + 50 + ";");
            System.out.println("Elapsed Time: " + (end - start)  + " ms\n");

            start = System.currentTimeMillis();
            s.executeQuery("SELECT a1,a2 FROM test3 where a1=" + 50 + ";");
            end = System.currentTimeMillis();
            System.out.println("Query: " + "SELECT a1,a2 FROM test3 where a1=" + 50 + ";");
            System.out.println("Elapsed Time: " + (end - start)  + " ms\n");

            start = System.currentTimeMillis();
            s.executeQuery("SELECT a1,a2 FROM test4 where a1=" + 50 + ";");
            end = System.currentTimeMillis();
            System.out.println("Query: " + "SELECT a1,a2 FROM test4 where a1=" + 50 + ";");
            System.out.println("Elapsed Time: " + (end - start)  + " ms\n");

            // Join tests
            System.out.println("\nTask 3: JOIN TESTS\n");
            start = System.currentTimeMillis();
            s.executeQuery("SELECT a1,a2,a3,a4 FROM test5, test1 where a3=a1;");
            end = System.currentTimeMillis();
            System.out.println("Query: " + "SELECT a1,a2,a3,a4 FROM test5, test1 where a3=a1;");
            System.out.println("Elapsed Time: " + (end - start)  + " ms\n");

            start = System.currentTimeMillis();
            s.executeQuery("SELECT a1,a2,a3,a4 FROM test5, test2 where a3=a1;");
            end = System.currentTimeMillis();
            System.out.println("Query: " + "SELECT a1,a2,a3,a4 FROM test5, test2 where a3=a1;");
            System.out.println("Elapsed Time: " + (end - start)  + " ms\n");

            start = System.currentTimeMillis();
            s.executeQuery("SELECT a1,a2,a3,a4 FROM test5, test3 where a3=a1;");
            end = System.currentTimeMillis();
            System.out.println("Query: " + "SELECT a1,a2,a3,a4 FROM test5, test3 where a3=a1;");
            System.out.println("Elapsed Time: " + (end - start)  + " ms\n");

            start = System.currentTimeMillis();
            s.executeQuery("SELECT a1,a2,a3,a4 FROM test5, test4 where a3=a1;");
            end = System.currentTimeMillis();
            System.out.println("Query: " + "SELECT a1,a2,a3,a4 FROM test5, test4 where a3=a1;");
            System.out.println("Elapsed Time: " + (end - start)  + " ms\n");

            conn.close();

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
