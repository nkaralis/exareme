package madgik.exareme.master.queryProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import madgik.exareme.master.queryProcessor.decomposer.ViewInfo;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.Column;
import madgik.exareme.master.queryProcessor.decomposer.query.NonUnaryWhereCondition;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.decomposer.query.Output;
import madgik.exareme.master.queryProcessor.decomposer.query.QueryUtils;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.decomposer.util.InterfaceAdapter;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

public class MySQLTester {

	private static boolean mysql=true;
	private static boolean exec=false;
	public static void main(String[] args) throws Exception {
		getDFLsFromDir("/home/dimitris/Dropbox/npdsql/alllubmall/");
		//  getDFLsFromDir("/home/dimitris/Dropbox/npdsql/existLast/");
		/*
		 * NodeHashValues hashes=new NodeHashValues(); NodeSelectivityEstimator
		 * nse = null; try { nse = new
		 * NodeSelectivityEstimator("/media/dimitris/T/exaremenpd100/" +
		 * "histograms.json"); } catch (Exception e) {
		 * 
		 * } hashes.setSelectivityEstimator(nse); SQLQuery query =
		 * SQLQueryParser.parse(testPlan, hashes); QueryDecomposer d = new
		 * QueryDecomposer(query, "/tmp/", 1, hashes);
		 * 
		 * d.setN2a(new NamesToAliases()); StringBuffer sb=new StringBuffer();
		 * for (SQLQuery s : d.getSubqueries()) { sb.append("\n");
		 * sb.append(s.toDistSQL()); } System.out.println(sb.toString());
		 */

	}

	private static void getDFLsFromDir(String dir) throws Exception {
		for (String file : readFilesFromDir(dir)) {
			System.out.println("file:" + file);
			getDFLFromFile(file);
		}
	}

	private static void getDFLFromFile(String file) throws Exception {
		String q = readFile(file);
		if (q.isEmpty()) {
			return;
		}
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
		SQLQuery query = SQLQueryParser.parse(q, hashes);
		QueryDecomposer d = new QueryDecomposer(query, "/tmp/", 1, hashes);
		d.setViewInfos(viewinfos);

		d.setN2a(new NamesToAliases());
		StringBuffer sb = new StringBuffer();
		String ex="";
		List<SQLQuery> result = d.getSubqueries();
		//boolean mysql = false;
		SQLQuery last = result.get(result.size() - 1);
		Class.forName("com.mysql.jdbc.Driver");
		Class.forName("org.postgresql.Driver");
		Connection connection = null;
		if(mysql){
		connection = DriverManager.getConnection(
		   "jdbc:mysql://127.0.0.1:3306/lubm100", "root", "gray769watt724!@#");
		}
		else{
			connection = DriverManager.getConnection(
					   "jdbc:postgresql://localhost:5432/lubm100","postgres", "gray769watt724!@#");
		}
		
		if (last.getUnionqueries().isEmpty()) {
			if (mysql) {
				for (Output out : last.getOutputs()) {
					Operand o = out.getObject();
					out.setObject(QueryUtils.convertToMySQLDialect(o));
				}
				for (NonUnaryWhereCondition bwc : last.getBinaryWhereConditions()) {
					for (Operand o : bwc.getOperands()) {
						o = QueryUtils.convertToMySQLDialect(o);
					}
				}
			}
			else{
				for (Column c : last.getAllColumns()) {
					if (!c.getName().startsWith("\"")) {
						c.setName("\"" + c.getName() + "\"");
					}
				}
				for (Table t : last.getInputTables()) {
					if (!t.getName().startsWith("\"")) {
						t.setName("\"" + t.getName() + "\"");
					}
				}
			}
			sb.append(last.toSQL());
			writeFile(file + ".dfl", last.toSQL());
		} else {
			sb.append("SELECT * FROM ( \n");
			
			String del = "";
			for (SQLQuery s : last.getUnionqueries()) {
				if (mysql) {
					for (Output out : s.getOutputs()) {
						Operand o = out.getObject();
						out.setObject(QueryUtils.convertToMySQLDialect(o));
					}
					for (NonUnaryWhereCondition bwc : s.getBinaryWhereConditions()) {
						for (Operand o : bwc.getOperands()) {
							o = QueryUtils.convertToMySQLDialect(o);
						}
					}
				}
				else{
					for (Column c : s.getAllColumns()) {
						if (!c.getName().startsWith("\"")) {
							c.setName("\"" + c.getName() + "\"");
						}
					}
					for (Table t : s.getInputTables()) {
						if (!t.getName().startsWith("\"")) {
							t.setName("\"" + t.getName() + "\"");
						}
					}
				}
				sb.append("\n");
				sb.append(del);
				String sql = s.toSQL();
				sb.append(sql.substring(0, sql.length() - 1));
				del = " UNION ALL \n";
			}
			sb.append(" \n ) q");
			writeFile(file + ".dfl", sb.toString());
			
			
		}
		if(!mysql){
			ex=sb.toString().replaceAll("`", "\'");
		}
		else{
			ex=sb.toString();
		}
		if(exec)
		execute(file, ex, connection);
		connection.close();
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
		List<String> files = new ArrayList<String>();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile() && listOfFiles[i].getCanonicalPath().endsWith("lubm9.q.sql")) {
				files.add(listOfFiles[i].getCanonicalPath());
			}
		}
		return files.toArray(new String[files.size()]);
	}

	public static void writeFile(String filename, String string) {
		writeFile(filename, string.getBytes());
	}

	public static void writeFile(String filename, byte[] string) {
		try {
			File file = new File(filename);
			file.getParentFile().mkdirs();
			OutputStream out = new FileOutputStream(file);
			out.write(string);
			out.close();
		} catch (Exception e) {
			System.err.println("Error writing file: " + filename);
			e.printStackTrace();
		}
	}
	public static void execute(String name, String query, Connection connection) throws SQLException{
		connection.setAutoCommit(false);
		
	Statement st=connection.createStatement();
	if(mysql)
	st.setFetchSize(Integer.MIN_VALUE);
	else
		st.setFetchSize(10000);
		long t1=System.currentTimeMillis();
		ResultSet rs=st.executeQuery(query);
		int results=0;
		while(rs.next()){
			//Object a=rs.getObject(1);
			results++;
		}
		rs.close();
		st.close();
		System.out.println("Query "+name+ " executed in Mysql in "+ (System.currentTimeMillis()-t1) +" ms ");
		System.out.println("No of results:"+results);
	}
	

}