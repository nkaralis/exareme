package madgik.exareme.master.queryProcessor.decomposer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.sqlite.SQLiteOpenMode;
import org.sqlite.javax.SQLiteConnectionPoolDataSource;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import madgik.exareme.jdbc.federated.AdpDriver;
import madgik.exareme.master.engine.executor.FinalUnionExecutor;
import madgik.exareme.master.engine.executor.ResultBuffer;
import madgik.exareme.master.engine.executor.SQLiteLocalExecutor;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.decomposer.util.InterfaceAdapter;
import madgik.exareme.master.queryProcessor.estimator.NodeInfo;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

public class QueryTester {

	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
		String dir="/home/dimitris/Dropbox/npdsql/duplicates/ex4/";
		//String dir="/home/dimitris/sqlitenpd";
		Map<String, String> queries=new HashMap<String, String>();
		String[] files=readFilesFromDir(dir);
		for(String file:files){
			queries.put(file, readFile(file));
		}
		boolean postgres=false;
		boolean exareme=false;
		boolean sqlite=false;
		boolean mysql=false;
		boolean estimate=false;
		boolean sparql=true;
		
		if(exareme){
			//Driver test=new AdpDriver();
			Class.forName("madgik.exareme.jdbc.federated.AdpDriver");
			Connection connection=DriverManager.getConnection("jdbc:fedadp:http://127.0.0.1:9090/media/dimitris/T/exaremenpd500new/");
			Statement s=connection.createStatement();
			for(String file:files){
				String query=queries.get(file);
				try{
				long t1=System.currentTimeMillis();
				ResultSet rs=s.executeQuery(query);
				int results=0;
				while(rs.next()){
					results++;
				}
				rs.close();
				System.out.println("Query "+file+ " executed in Exareme in "+ (System.currentTimeMillis()-t1) +" ms ");
				System.out.println("No of results:"+results);
				}catch(Exception e){
					System.out.println("error in query "+file);
					System.out.println(e.getMessage());
				}
			}
			connection.close();
		}
		if(postgres){
			Class.forName("org.postgresql.Driver");
			Connection connection = null;
			connection = DriverManager.getConnection(
			   "jdbc:postgresql://localhost:5432/lubm100","postgres", "gray769watt724!@#");
			
			connection.setAutoCommit(false);
			//outofmemory
			
			Statement s=connection.createStatement();
			s.setFetchSize(10000);
			//outOfMemory error
			
			for(String file:files){
				long t1=System.currentTimeMillis();
				String query=queries.get(file);
				String[] splitted =query.split(";;");
				for(int i=0;i<splitted.length-1;i++){
					s.execute(splitted[i]);
				}
				System.out.println(System.currentTimeMillis()-t1) ;
				ResultSet rs=s.executeQuery(splitted[splitted.length-1]);
				//s.execute(query);
				int results=0;
				while(rs.next()){
					//Object a=rs.getObject(1);
					results++;
				}
				rs.close();
				System.out.println("Query "+file+ " executed in Postgres in "+ (System.currentTimeMillis()-t1) +" ms ");
				System.out.println("No of results:"+results);
			}
			connection.close();
		}
		if(mysql){
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = null;
			connection = DriverManager.getConnection(
			   "jdbc:mysql://127.0.0.1:3306/lubm100", "root", "gray769watt724!@#");
			Statement s=connection.createStatement();
			for(String file:files){
				long t1=System.currentTimeMillis();
				String query=queries.get(file);
				String[] splitted =query.split(";;");
				for(int i=0;i<splitted.length-1;i++){
					s.execute(splitted[i]);
				}
				System.out.println(System.currentTimeMillis()-t1) ;
				ResultSet rs=s.executeQuery(splitted[splitted.length-1]);
				//s.execute(query);
				int results=0;
				while(rs.next()){
					//Object a=rs.getObject(1);
					results++;
				}
				rs.close();
				System.out.println("Query "+file+ " executed in Mysql in "+ (System.currentTimeMillis()-t1) +" ms ");
				System.out.println("No of results:"+results);
			}
			connection.close();
		}
		if(sqlite){
			dir="/home/dimitris/sqlitenpd";
			queries.clear();
			for(String file:readFilesFromDir(dir)){
				queries.put(file, readFile(file));
			}
			Class.forName("org.sqlite.JDBC");
			 org.sqlite.SQLiteConfig config = new org.sqlite.SQLiteConfig();
			 config.enableLoadExtension(true);
			 //config.setCacheSize(2400000);
			 //config.setPageSize(4096);
			 //config.setLockingMode(mode);
			 SQLiteConnectionPoolDataSource dataSource = new SQLiteConnectionPoolDataSource();
			    dataSource.setUrl("jdbc:sqlite:/media/dimitris/T/tmp/a.db");
			// dataSource.setUrl("jdbc:sqlite::memory:");
			    dataSource.setConfig(config);
			Connection connection=dataSource.getConnection();//DriverManager.getConnection("jdbc:sqlite:test.db");
			
			//outofmemory
			
			
			
			connection.createStatement().execute("PRAGMA synchronous = OFF");
			connection.createStatement().execute("PRAGMA ignore_check_constraints = true");
			connection.createStatement().execute("PRAGMA locking_mode = EXCLUSIVE");
			connection.createStatement().execute("PRAGMA automatic_index = TRUE");
			connection.createStatement().execute("PRAGMA auto_vacuum = NONE"); 
			connection.createStatement().execute("PRAGMA page_size = 4096");
			connection.createStatement().execute("PRAGMA cache_size = 2400000");
			
			//connection.createStatement().execute("pragma temp_store=MEMORY");
			//connection.createStatement().execute("PRAGMA temp_store_directory = '/media/dimitris/T/tmp/'");
			
			Statement s=connection.createStatement();
			//s.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/licence_licensee_hst.0.db?cache=shared' AS ll ");
			//s.execute("attach database '/media/dimitris/T/exaremenpd500new/licence_licensee_hst.0.db' as ll");
			connection.setAutoCommit(false);
			//s.setFetchSize(10000);
			//s.execute("PRAGMA cache_size = 600000");
			connection.createStatement().execute("PRAGMA journal_mode = OFF");
			s.setFetchSize(100000);
			/*s.execute("attach database '/media/dimitris/T/exaremenpd100/company.0.db' as company");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/licence.0.db' as licence");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/strat_litho_wellbore_core.0.db' as strat_litho_wellbore_core");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_core.0.db' as wellbore_core");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_development_all.0.db' as wellbore_development_all");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_exploration_all.0.db' as wellbore_exploration_all");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_npdid_overview.0.db' as wellbore_npdid_overview");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_shallow_all.0.db' as wellbore_shallow_all");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/discovery.0.db' as discovery");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/field.0.db' as field");*/
			
			
			
			for(String file:queries.keySet()){
				String query=queries.get(file);
				//String query="pragma temp_store";
				try{
				long t1=System.currentTimeMillis();
				ResultSet rs=s.executeQuery(query);
				int results=0;
				if(rs.next()){
					System.out.println(rs.getObject(1).toString());
				}
				while(rs.next()){
					//System.out.println(rs.getObject(4));
					results++;
				}
				rs.close();
				System.out.println("Query "+file+ " executed in sqlite in "+ (System.currentTimeMillis()-t1) +" ms ");
				System.out.println("No of results:"+results);
				}catch(Exception e){
					System.out.println("error in query "+file);
					System.out.println(e.getMessage());
				}
			}
			connection.close();
		}
		if(estimate){
			long sttt=System.currentTimeMillis();
			for(String file:files){
				try {
				NodeHashValues hashes = new NodeHashValues();

				NodeSelectivityEstimator nse = null;
				Map<Set<String>, Set<ViewInfo>> viewinfos = new HashMap<Set<String>, Set<ViewInfo>>();
				try {
					nse = new NodeSelectivityEstimator("/media/dimitris/T/exaremelubm100/" + "histograms.json");
					
					BufferedReader br;
					br = new BufferedReader(new FileReader("/home/dimitris/Dropbox/npdsql/duplicates/" + "views.json"));
					
					Gson gson = new GsonBuilder().registerTypeAdapter(Operand.class, new InterfaceAdapter<Operand>()).create();
					java.lang.reflect.Type viewType = new TypeToken<Map<String, Set<ViewInfo>>>() {
					}.getType();
					viewinfos = gson.fromJson(br, viewType);
				
				} catch (Exception e) {
					e.printStackTrace(System.out);
				}
				hashes.setSelectivityEstimator(nse);
				//System.out.println(System.currentTimeMillis()-sttt);
				SQLQuery query = SQLQueryParser.parse( readFile(file), hashes);
				//System.out.println(System.currentTimeMillis()-sttt);
				QueryDecomposer d = new QueryDecomposer(query, "/tmp/", 1, hashes);
				//System.out.println(System.currentTimeMillis()-sttt);
				d.setN2a(new NamesToAliases());
				// String table="avavav";
				String table = query.getInputTables().get(0).getName().toLowerCase();
				NodeInfo r = d.getSelectivityForTopNode();

				
					double dupl = NodeSelectivityEstimator.getDuplicateEstimation(r, new HashSet<String>());
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		if(sparql){
				
				
				
				List<SQLQuery> subqueries=new ArrayList<SQLQuery>();
				for(int t=1;t<17;t++){
							SQLQuery a=new SQLQuery();
							a.setStringSQL();
							a.setSQL("select count(*) from "
												+" R"+t+" st cross join w where st.o=w.s;\n");
							subqueries.add(a);
							 
				}
						     long start=System.currentTimeMillis();
							ExecutorService es = Executors.newFixedThreadPool(2);
							Map<String, SQLQuery> tableNames=new HashMap<String, SQLQuery>();
							for(int i=0;i<subqueries.size()-1;i++){
								SQLQuery next=subqueries.get(i);
								tableNames.put(next.getTemporaryTableName(), next);
								
							}
							//Connection ccc=getConnection("");
							List<SQLiteLocalExecutor> executors=new ArrayList<SQLiteLocalExecutor>();
							ResultBuffer globalBuffer=new ResultBuffer();
							Set<SQLQuery> finishedQueries=new HashSet<SQLQuery>();
							for (int i = 0; i < subqueries.size(); i++) {
								SQLQuery q = subqueries.get(i);
								Set<SQLQuery> dependencies=new HashSet<SQLQuery>();
								for(Table t:q.getInputTables()){
									if(tableNames.containsKey(t.getName())){
										dependencies.add(tableNames.get(t.getName()));
									}
								}
								
								SQLiteLocalExecutor ex=new SQLiteLocalExecutor(q, getConnection(""), true, dependencies, finishedQueries);
								ex.setGlobalBuffer(globalBuffer);
								executors.add(ex);
									
							}
							
							
							/*SQLQuery u=subqueries.get(subqueries.size()-1);
							SQLQuery firstUnion=u.getUnionqueries().get(0);
							Connection c2=getConnection("union");
							c2.setAutoCommit(false);
							Statement st2=c2.createStatement();
							
							String create="create table "+u.getTemporaryTableName()+" (";
							String prepare="insert into "+u.getTemporaryTableName()+" values (";
							String del="";
							for(String out:firstUnion.getOutputAliases()){
								create+=del+out;
								prepare+=del+"?";
								del=",";					
								
							}
							create+=" )";
							prepare+=" )";
							System.out.println(create);
							st2.execute(create);
							st2.close();
							
							PreparedStatement st=c2.prepareStatement(prepare);*/
							FinalUnionExecutor ex=new FinalUnionExecutor(globalBuffer, null, subqueries.size());
							es.execute(ex);
							for(SQLiteLocalExecutor exec:executors){
								es.execute(exec);
							}
							es.shutdown();
							try {
								boolean finished = es.awaitTermination(300, TimeUnit.MINUTES);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
						//	if(finished){
						//		c2.commit();
						//	connection.commit();
							
							System.out.println(System.currentTimeMillis()-start);//}
			}

						
							
						
				
		
	}
	
	private static String readFile(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");

		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
			stringBuilder.append(ls);
		}
		reader.close();
		return stringBuilder.toString();
	}
	
    private static String[] readFilesFromDir(String string) throws IOException {
    	File folder = new File(string);
    	File[] listOfFiles = folder.listFiles();
    	List<String> files=new ArrayList<String>();
    	    for (int i = 0; i < listOfFiles.length; i++) {
    	      if (listOfFiles[i].isFile()&&listOfFiles[i].getCanonicalPath().endsWith("query.sql") ) {
    	     // if(listOfFiles[i].getCanonicalPath().endsWith("01.q.sql"))
    	    //		  continue;
    	   //   if(listOfFiles[i].getCanonicalPath().endsWith("25.q.sql"))
	    	//	  continue;
    	     // if(listOfFiles[i].getCanonicalPath().endsWith("04.q.sql"))
	    		//  continue;
    	    //	  if(!listOfFiles[i].getCanonicalPath().endsWith("9test"))
    	    	//	 continue;
    	    	  files.add(listOfFiles[i].getCanonicalPath());
    	      }
    	   
    	    }
    	    java.util.Collections.sort(files);
    	    return files.toArray(new String[files.size()]);
	}

    
    private static Connection getConnection(String postfix){
		try{
		Class.forName("org.sqlite.JDBC");
		 org.sqlite.SQLiteConfig config = new org.sqlite.SQLiteConfig();
		 //config.setSharedCache(true);
	config.enableLoadExtension(true);
		// config.setOpenMode(SQLiteOpenMode.READONLY);
		// config.setOpenMode(SQLiteOpenMode.CREATE);
		 //config.setOpenMode(SQLiteOpenMode.NOMUTEX);
		 //config.setOpenMode(SQLiteOpenMode.SHAREDCACHE);
		 //config.setSharedCache(true);
		 
		 config.setCacheSize(2400000);
		 config.setPageSize(4096);
		 //config.setLockingMode(mode);
		 SQLiteConnectionPoolDataSource dataSource = new SQLiteConnectionPoolDataSource();
		  //dataSource.setUrl("jdbc:sqlite:file:memdb1?mode=memory&nolock=1");
		dataSource.setUrl("jdbc:sqlite:file:/media/dimitris/T/tmp/pxbpwj"+postfix+"?nolock=1");
		// dataSource.setUrl("jdbc:sqlite::memory:");
		    dataSource.setConfig(config);
		    //=dataSource.getConnection();//
		//Connection connection=DriverManager.getConnection("jdbc:sqlite:file:/media/dimitris/T/tmp/asrrsasasas");
		Connection connection=dataSource.getConnection();
		//outofmemory
		
		
		Statement stmt=connection.createStatement();
		//stmt.execute("PRAGMA locking_mode = EXCLUSIVE");
		stmt.execute("PRAGMA threads = 8");
		stmt.execute("PRAGMA auto_vacuum = NONE"); 
		stmt.execute("PRAGMA journal_mode = OFF");
		stmt.execute("PRAGMA synchronous = OFF");
		stmt.execute("PRAGMA ignore_check_constraints = true");
		//stmt.execute("PRAGMA read_uncommitted = true");
		stmt.execute("PRAGMA locking_mode = EXCLUSIVE");
		stmt.execute("PRAGMA automatic_index = TRUE");
		stmt.execute("PRAGMA ignore_check_constraints = true");
		//stmt.execute("PRAGMA page_size = 4096");
		//stmt.execute("PRAGMA cache_size = 2400000");*/
		stmt.execute("select load_extension('/home/dimitris/virtualtables/wrapper')");
		//stmt.execute("PRAGMA journal_mode = WAL");
		//stmt.execute("PRAGMA read_uncommited = TRUE");
		///
		//connection.createStatement().execute("pragma temp_store=MEMORY");
		//connection.createStatement().execute("PRAGMA temp_store_directory = '/media/dimitris/T/tmp/'");
		//
		//Set<String> tblnames=new HashSet<String>();
		/*for(int i=0;i<subqueries.size()-1;i++){
			SQLQuery q=subqueries.get(i);
			for(Table t:q.getInputTables()){
				if(!t.getName().startsWith("table")){
					if(t.getName().startsWith("siptable")){
						t.setName(t.getAlias());
						if(tblnames.add(t.getName()))
						stmt.execute("create virtual table "+t.getName()+" using unionsiptext");
					}
					else{
						if(tblnames.add(t.getName()))
						stmt.execute("ATTACH 'file:"+path+t.getName()+".0.db?nolock=1' AS "+ t.getName());
					}
					}

			}
		}*/
		stmt.execute("ATTACH 'file:/media/dimitris/T/test/rdf.db?nolock=1' AS siptable0"); 
		//stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/strat_litho_wellbore_core.0.db?immutable=1&cache=private&nolock=1' AS wellbssore0");
		//stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/licence.0.db?cache=shared' AS licence0"); 
		//stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/wellbore.0.db?cache=shared' AS wellbore0"); 
	//	stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/wellbore_core.0.db?immutable=1&cache=private&nolock=1' AS wellbore_core0"); 
	//	stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/wellbore_exploration_all.0.db?immutable=1&cache=private&nolock=1' AS wellbore_exploration_all0"); 
	//	stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/wellbore_npdid_overview.0.db?immutable=1&cache=private&nolock=1' AS wellbore_npdid_overview0"); 
		//stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/company.0.db?cache=shared' AS aare0");
		//		stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/wellbore_shallow_all.0.db?immutable=1&cache=private&nolock=1' AS aadre0");
			//			stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/wellbore_development_all.0.db?immutable=1&cache=private&nolock=1' AS aaress0");
		//						stmt.execute("ATTACH 'file:/media/dimitris/T/exaremenpd500new/field.0.db?immutable=1&cache=private&nolock=1' AS aarvve0");
		
	
								
								stmt.close();
							//	stmt.execute("create virtual table if not exists ttt using unionsiptext");
							//	ResultSet rs=stmt.executeQuery("select * from ttt where x=1000");
								//while(rs.next()){
							//		System.out.println("sssssss");
							//	}
	//	connection.setAutoCommit(false);
		//s.setFetchSize(10000);
		//s.execute("PRAGMA cache_size = 600000");
		return connection;
		}
		catch(java.sql.SQLException | ClassNotFoundException e){
			System.out.println(e.getMessage());
			return null;
		}
		
	}
    
}
