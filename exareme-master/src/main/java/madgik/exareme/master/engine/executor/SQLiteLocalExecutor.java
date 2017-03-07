package madgik.exareme.master.engine.executor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.sqlite.javax.SQLiteConnectionPoolDataSource;

import java.sql.PreparedStatement;

import madgik.exareme.master.queryProcessor.decomposer.federation.DB;
import madgik.exareme.master.queryProcessor.decomposer.federation.DataImporter;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;

public class SQLiteLocalExecutor implements Runnable {
	private Connection con;
	private SQLQuery s;
	private boolean temp;
	private Set<SQLQuery> dependencies;
	private Set<SQLQuery> finishedQueries;
	private ResultBuffer globalBuffer;
	private static final Logger log = Logger.getLogger(DataImporter.class);

	public void setGlobalBuffer(ResultBuffer globalBuffer) {
		this.globalBuffer = globalBuffer;
	}

	public SQLiteLocalExecutor(SQLQuery q, Connection c, boolean t, Set<SQLQuery> d, Set<SQLQuery> f) {
		this.s = q;
		this.con = c;
		this.temp = t;
		this.dependencies = d;
		this.finishedQueries = f;
	}

	@Override
	public void run() {
		if (this.dependencies.isEmpty()) {
			execute();
		} else {
			synchronized (finishedQueries) {
				while (!finishedQueries.containsAll(dependencies)) {
					System.out.println("trying! finished:"+finishedQueries+" depend.:"+dependencies);
					try {
						finishedQueries.wait();
					} catch (InterruptedException e) {
					}
				}
			}
			execute();
		}
		//if (!temp) {
			synchronized (finishedQueries) {
				finishedQueries.add(s);
				finishedQueries.notifyAll();
			}
		//}

	}

	private void execute() {
		Statement st;
		try {
			System.out.println("start");
			//st=con.createStatement();
			if (temp) {
				//System.out.println(s.toSQL());
				// st.execute("ATTACH DATABASE '' as
				// "+s.getTemporaryTableName()+"0");
				//st.execute("ATTACH 'file:/media/dimitris/T/tmp/" + s.getTemporaryTableName() + "0.db' as "
					//	+ s.getTemporaryTableName() + "0");
				//st.execute("create table " + s.getTemporaryTableName() + "0" + "." + s.getTemporaryTableName() + " as "
				//		+ s.toSQL());
				con.setAutoCommit(false);
				st = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY);
				st.setFetchSize(10000);
				long lll=System.currentTimeMillis();
				ResultSet rs=st.executeQuery(s.toSQL());
				int columns=rs.getMetaData().getColumnCount();
				List<List<Object>> localBuffer=new ArrayList<List<Object>>(1000);
				int counter=0;
				while(rs.next()){
					//if(counter==1){
					//	System.out.println("started");
					//}
					if(counter==999){
						counter=0;
						synchronized(globalBuffer){
							while(globalBuffer.size()>9000){
								try {
									globalBuffer.wait();
								} catch (InterruptedException e) {
									//System.out.println("local inter");
								}
							}
							System.out.println("adding batch");
							globalBuffer.addAll(localBuffer);
							globalBuffer.notifyAll();
						}
						localBuffer.clear();
					}
					List<Object> tuple=new ArrayList<Object>(columns);
					for(int i=1;i<columns+1;i++){
						tuple.add(rs.getObject(i));
					}
				localBuffer.add(tuple);
					counter++;
				}
				System.out.println("time:"+(lll-System.currentTimeMillis()));
				synchronized(globalBuffer){
					while(globalBuffer.size()>9000){
						try {
							globalBuffer.wait();
						} catch (InterruptedException e) {
						}
					}
					globalBuffer.addAll(localBuffer);
					globalBuffer.addFinished();
					globalBuffer.notifyAll();
				}
				st.close();
				//con.close();
				localBuffer.clear();
				
			} else {
				// System.out.println(s.toSQL());
				PreparedStatement ps = con.prepareStatement(s.toSQL());
				ps.execute();
			}
			
			//connection.close();
			System.out.println("end");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		
	}
}