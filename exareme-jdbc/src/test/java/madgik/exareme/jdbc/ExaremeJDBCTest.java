package madgik.exareme.jdbc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.*;

/**
 * @author alex
 */
public class ExaremeJDBCTest {
    private static final Logger log = Logger.getLogger(ExaremeJDBCTest.class);

    public static void printResultset(ResultSet rs) throws SQLException, FileNotFoundException, UnsupportedEncodingException {
    	PrintWriter writer = new PrintWriter("/home/nkaralis/Datasets/Jackpine/results/ids/parts8/aoa.txt", "UTF-8");
    	
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
                writer.print(v + "\t");
            }
            log.info("");
            ++count;
            writer.println("");
        }
        System.out.println("Count: " + count + "\n\tSize: " + size);
        writer.close();
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
        String q1 = "select a1.pk_uid, a2.pk_uid from  area8 a1 , area8 a2 , spatialindex s where a1.pk_uid < a2.pk_uid and ST_Overlaps(a1.geomcol, a2.geomcol) "
				+ "and a2.rowid = s.rowid and s.f_table_name = 'area8' and s.search_frame = a1.geomcol";
        String q2 = "select pk_uid from arealm_merge where ST_Distance(geomcol, ST_GeomFromText('POINT(-97.7 30.30)')) < 1000";
        String q3 = "distributed drop table points1;";
        String q4 = "distributed create table edges1 to 1 on geometry as external "
      		  + "select pk_uid, statefp, tlid, tfidl, tfidr, mtfcc, fullname, smid, lfromadd, rtoadd, zipl, zipr, featcat, hydroflg, railflg, roadflg, olfflg, passflg, divroad, exttyp, ttyp, deckedroad, artpath, persist, gcseflg, offsetl, offsetr, tnidf, tnidt, geometry from"
      		  + "(file file:/home/nkaralis/Datasets/Jackpine/edges_merge.tsv header:t) ;";
        String q5 = "distributed create table lilou5 as external select id, geometry from"
      		      + "(file file:/home/nkaralis/Datasets/geosparkresutls.tsv header:t) ;";;
        String q6 = "distributed create table results as select l1.geometry, l2.geometry from lilou4 l1, lilou4 l2 "
        		  + "where st_intersects(st_geomfromtext(l1.geometry),st_geomfromtext(l2.geometry)) = 1;";
        
        Statement st = conn.createStatement();
        log.info("Statement created.");

        ResultSet rs = st.executeQuery(q1);
        log.info("Query executed.");
        printResultset(rs);
        rs.close();
        st.close();
        log.info("--------- TEST -------------");

    }
}
