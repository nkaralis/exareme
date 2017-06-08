package madgik.exareme.jdbc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * @author alex
 */
public class ExaremeJDBCTest {
    private static final Logger log = Logger.getLogger(ExaremeJDBCTest.class);

    public static void printResultset(ResultSet rs) throws SQLException {
        log.info("Columns: " + rs.getMetaData().getColumnCount());
        int count = 0;
        int size = 0;
        for (int c = 0; c < rs.getMetaData().getColumnCount(); ++c) {
            log.info(rs.getMetaData().getColumnName(c + 1));
        }
        while (rs.next()) {
            String[] next = new String[rs.getMetaData().getColumnCount()];
            for (int c = 0; c < next.length; ++c) {
                next[c] = "" + rs.getObject(c + 1);
                size += next[c].length();
            }
            for (String v : next) {
                log.info(v + "\t");
            }
            log.info("");
            ++count;
        }
        System.out.println("Count: " + count + "\n\tSize: " + size);
    }

    @Before public void setUp() throws Exception {
        Logger.getRootLogger().setLevel(Level.ALL);

    }

    @Test public void testJDBC() throws Exception {
        log.info("--------- TEST -------------");
        // Load the driver
        Class.forName("madgik.exareme.jdbc.federated.AdpDriver");
        

        String database = "jdbc:fedadp:http://localhost:9090/home/nkaralis/exareme_db/test";
        Connection conn = DriverManager.getConnection(database);
        log.info("Connections created.");

        //String tablename = "sales";
        //String q="select * from "+tablename;
        String q1 = "select distinct l1.geometry as g1, l2.geometry as g2 from lilou4 l1, lilou5 l2 "
        		  + "where intersects(st_geomfromtext(l1.geometry),st_geomfromtext(l2.geometry)) = 1";
        String q2 = "select * from lilou4";
        String q3 = "distributed drop table lilou5;";
        String q4 = "distributed create table lilou5 to 2 on geomcol as external select id, geometry from"
        		  + "(file file:/home/nkaralis/Datasets/geosparkresutls.tsv header:t) ;";
        String q5 = "distributed create table lilou5 as external select id, geometry from"
      		      + "(file file:/home/nkaralis/Datasets/geosparkresutls.tsv header:t) ;";;
        String q6 = "distributed create table results as select l1.geometry, l2.geometry from lilou4 l1, lilou4 l2 "
        		  + "where st_intersects(st_geomfromtext(l1.geometry),st_geomfromtext(l2.geometry)) = 1;";
        
        Statement st = conn.createStatement();
        log.info("Statement created.");
        //ResultSet rs1 = st.executeQuery("select load_extension('/usr/local/lib/mod_spatialite')");

        ResultSet rs = st.executeQuery(q2);
        log.info("Query executed.");
        printResultset(rs);
        rs.close();
        st.close();
        log.info("--------- TEST -------------");

    }
}
