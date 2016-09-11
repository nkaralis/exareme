package madgik.exareme.master.engine.executor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import java.sql.PreparedStatement;

import madgik.exareme.master.queryProcessor.decomposer.federation.DB;
import madgik.exareme.master.queryProcessor.decomposer.federation.DataImporter;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;

public class SQLiteLocalExecutor implements Runnable {
	private Connection con;
	private SQLQuery s;
	private boolean temp;
	private static final Logger log = Logger.getLogger(DataImporter.class);

	public SQLiteLocalExecutor(SQLQuery q, Connection c, boolean t) {
		this.s = q;
		this.con=c;
		this.temp=t;
	}

	@Override
	public void run() {
		
		Statement st;
		try {
			System.out.println("start");
			st = con.createStatement();
			if(temp){
				st.execute(s.toSQL());
			}
			else{
				PreparedStatement ps=con.prepareStatement(s.toSQL());
				ps.execute();
			}
			st.close();
			System.out.println("end");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		
		
	}
	
	}