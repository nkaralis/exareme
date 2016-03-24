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
import java.util.List;
import java.util.Map;

import madgik.exareme.jdbc.federated.AdpDriver;

public class QueryTester {

	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
		String dir="/home/dimitris/npdsql/existential/";
		//String dir="/home/dimitris/sqlitenpd";
		Map<String, String> queries=new HashMap<String, String>();
		for(String file:readFilesFromDir(dir)){
			queries.put(file, readFile(file));
		}
		boolean postgres=false;
		boolean exareme=true;
		boolean sqlite=false;
		if(postgres){
			Class.forName("org.postgresql.Driver");
			Connection connection = null;
			connection = DriverManager.getConnection(
			   "jdbc:postgresql://localhost:5432/npd_vig_scale100","postgres", "gray769watt724!@#");
			Statement s=connection.createStatement();
			for(String file:queries.keySet()){
				String query=queries.get(file);
				long t1=System.currentTimeMillis();
				ResultSet rs=s.executeQuery(query);
				int results=0;
				while(rs.next()){
					results++;
				}
				rs.close();
				System.out.println("Query "+file+ " executed in Postgres in "+ (System.currentTimeMillis()-t1) +" ms ");
				System.out.println("No of results:"+results);
			}
			connection.close();
		}
		if(exareme){
			//Driver test=new AdpDriver();
			Class.forName("madgik.exareme.jdbc.federated.AdpDriver");
			Connection connection=DriverManager.getConnection("jdbc:fedadp:http://127.0.0.1:9090/media/dimitris/T/exaremenpd100/");
			Statement s=connection.createStatement();
			for(String file:queries.keySet()){
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
		if(sqlite){
			dir="/home/dimitris/sqlitenpd";
			queries.clear();
			for(String file:readFilesFromDir(dir)){
				queries.put(file, readFile(file));
			}
			Class.forName("org.sqlite.JDBC");
			Connection connection=DriverManager.getConnection("jdbc:sqlite:test.db");
			Statement s=connection.createStatement();
			s.execute("attach database '/media/dimitris/T/exaremenpd100/company.0.db' as company");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/strat_litho_wellbore_core.0.db' as strat_litho_wellbore_core");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_core.0.db' as wellbore_core");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_development_all.0.db' as wellbore_development_all");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_exploration_all.0.db' as wellbore_exploration_all");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_npdid_overview.0.db' as wellbore_npdid_overview");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/wellbore_shallow_all.0.db' as wellbore_shallow_all");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/discovery.0.db' as discovery");
			s.execute("attach database '/media/dimitris/T/exaremenpd100/field.0.db' as field");
			
			for(String file:queries.keySet()){
				String query=queries.get(file);
				try{
				long t1=System.currentTimeMillis();
				ResultSet rs=s.executeQuery(query);
				int results=0;
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
    	      if (listOfFiles[i].isFile()&&listOfFiles[i].getCanonicalPath().endsWith(".sql")) {
    	    	  if(!listOfFiles[i].getCanonicalPath().endsWith("06.q.sql"))
    	    		  continue;
    	    	  files.add(listOfFiles[i].getCanonicalPath());
    	      }
    	    }
    	    return files.toArray(new String[files.size()]);
	}

}
