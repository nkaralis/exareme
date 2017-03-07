package madgik.exareme.master.queryProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.StringBuffer;
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

import org.sqlite.javax.SQLiteConnectionPoolDataSource;

public class QueryTester {

	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
		// String dir="/home/dimitris/Dropbox/npdsql/npdnew100/";
		// String dir="/home/dimitris/sqlitenpd";
		String dir = "/home/dimitris/Dropbox/npdsql/alllubm/";
		String postgres = args[0];
		String exareme = args[1];
		String sqlite = args[2];
		String scale = args[3];
		String endsWith = args[4];
		String outfile = args[5];
		String mysql = "1";
		System.out.println("starting...");
		Map<String, String> queries = new HashMap<String, String>();
		String[] files = readFilesFromDir(dir, endsWith);

		// queries.put(file, readFile(file));
		// }

		StringBuffer out = new StringBuffer();
		out.append("Starting...\n");

		if (exareme.equals("1")) {
			// Driver test=new AdpDriver();
			Class.forName("madgik.exareme.jdbc.federated.AdpDriver");
			Connection connection = DriverManager
					.getConnection("jdbc:fedadp:http://127.0.0.1:9090/media/dimitris/T/" + scale + "/");

			Statement s = connection.createStatement();
			for (String file : files) {
				String query = readFile(file);
				try {
					System.out.println("droping cache...");
					out.append("droping cache...");

					String[] commands = { "/bin/sh", "-c", "echo 3 > /proc/sys/vm/drop_caches" };
					Process proc = Runtime.getRuntime().exec(commands);
					// proc.waitFor();
					BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

					BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

					// read the output from the command
					System.out.println("Here is the standard output of the command:\n");
					String s2 = null;
					while ((s2 = stdInput.readLine()) != null) {
						System.out.println(s2);
					}

					// read any errors from the attempted command
					System.out.println("Here is the standard error of the command (if any):\n");
					while ((s2 = stdError.readLine()) != null) {
						System.out.println(s2);
					}
					System.out.println("cache dropped");
					out.append("cache dropped");
					long t1 = System.currentTimeMillis();
					ResultSet rs = s.executeQuery(query);
					int results = 0;
					if (rs.next()) {
						results++;
						System.out.println("Query " + file + " first result in Exareme in: "
								+ (System.currentTimeMillis() - t1) + " ms ");
					}

					while (rs.next()) {
						results++;
					}
					rs.close();
					System.out.println(
							"Query " + file + " executed in Exareme in " + (System.currentTimeMillis() - t1) + " ms ");
					System.out.println("No of results:" + results);
					out.append("Query " + file + " executed in Exareme in " + (System.currentTimeMillis() - t1)
							+ " ms \n");
					out.append("No of results:" + results + "\n");

					System.out.println("2nd run with warm cache!");
					out.append("2nd run with warm cache!");
					t1 = System.currentTimeMillis();
					rs = s.executeQuery(query);
					results = 0;
					if (rs.next()) {
						results++;
						System.out.println("Query " + file + " first result in Exareme in: "
								+ (System.currentTimeMillis() - t1) + " ms ");
					}

					while (rs.next()) {
						results++;
					}
					rs.close();
					System.out.println(
							"Query " + file + " executed in Exareme in " + (System.currentTimeMillis() - t1) + " ms ");
					System.out.println("No of results:" + results);
					out.append("Query " + file + " executed in Exareme in " + (System.currentTimeMillis() - t1)
							+ " ms \n");
					out.append("No of results:" + results + "\n");

				} catch (Exception e) {
					System.out.println("error in query " + file);
					System.out.println(e.getMessage());
					out.append("error in query " + file);
					out.append(e.getMessage());
				}
			}
			connection.close();
		}
		if (postgres.equals("1")) {

			Class.forName("org.postgresql.Driver");
			Connection connection = null;

			System.out.println(queries);
			for (String file : files) {
				String query = readFile(file);
				System.out.println(file);
				try {

					System.out.println("stopping postgres...");
					out.append("stopping postgres...");

					String[] commands = { "/bin/sh", "-c", "service postgresql stop" };
					Process proc = Runtime.getRuntime().exec(commands);
					// proc.waitFor();
					BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

					BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

					// read the output from the command
					System.out.println("Here is the standard output of the command:\n");
					String s2 = null;
					while ((s2 = stdInput.readLine()) != null) {
						System.out.println(s2);
					}

					// read any errors from the attempted command
					System.out.println("Here is the standard error of the command (if any):\n");
					while ((s2 = stdError.readLine()) != null) {
						System.out.println(s2);
					}
					System.out.println("postgres stopped");
					out.append("postgres stopped");

					System.out.println("droping cache...");
					out.append("droping cache...");

					String[] commands2 = { "/bin/sh", "-c", "echo 3 > /proc/sys/vm/drop_caches" };
					proc = Runtime.getRuntime().exec(commands2);
					// proc.waitFor();
					stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

					stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

					// read the output from the command
					System.out.println("Here is the standard output of the command:\n");
					s2 = null;
					while ((s2 = stdInput.readLine()) != null) {
						System.out.println(s2);
					}

					// read any errors from the attempted command
					System.out.println("Here is the standard error of the command (if any):\n");
					while ((s2 = stdError.readLine()) != null) {
						System.out.println(s2);
					}
					System.out.println("cache dropped");
					out.append("cache dropped");

					;

					System.out.println("starting postgres...");
					out.append("starting postgres...");

					String[] commands3 = { "/bin/sh", "-c", "service postgresql start" };
					proc = Runtime.getRuntime().exec(commands3);
					// proc.waitFor();
					stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));

					stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

					// read the output from the command
					System.out.println("Here is the standard output of the command:\n");
					s2 = null;
					while ((s2 = stdInput.readLine()) != null) {
						System.out.println(s2);
					}

					// read any errors from the attempted command
					System.out.println("Here is the standard error of the command (if any):\n");
					while ((s2 = stdError.readLine()) != null) {
						System.out.println(s2);
					}
					System.out.println("postgres started");
					out.append("postgres started");
					connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/npd_new_scale" + scale,
							"postgres", "gray769watt724!@#");
					connection.setAutoCommit(false);
					// outofmemory

					Statement s = connection.createStatement();
					s.setFetchSize(10000);
					// Statement s=connection.createStatement();
					long t1 = System.currentTimeMillis();
					ResultSet rs = s.executeQuery(query);
					int results = 0;
					while (rs.next()) {
						// Object a=rs.getObject(1);
						results++;
					}
					rs.close();
					System.out.println("Query " + file + " executed in Postgres in " + (System.currentTimeMillis() - t1)
							+ " ms \n");
					System.out.println("No of results:" + results + "\n");
					out.append(
							"Query " + file + " executed in Postgres in " + (System.currentTimeMillis() - t1) + " ms ");
					out.append("No of results:" + results);

					System.out.println("2nd run with warm cache!");
					out.append("2nd run with warm cache!");
					t1 = System.currentTimeMillis();
					rs = s.executeQuery(query);
					results = 0;
					while (rs.next()) {
						// Object a=rs.getObject(1);
						results++;
					}
					rs.close();
					System.out.println("Query " + file + " executed in Postgres in " + (System.currentTimeMillis() - t1)
							+ " ms \n");
					System.out.println("No of results:" + results + "\n");
					out.append(
							"Query " + file + " executed in Postgres in " + (System.currentTimeMillis() - t1) + " ms ");
					out.append("No of results:" + results);
				} catch (Exception e) {
					System.out.println("error in query " + file);
					System.out.println(e.getMessage());
					out.append("error in query " + file);
					out.append(e.getMessage());
				}
			}
			connection.close();
		}
		if (mysql.equals("1")) {
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = null;
			connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/lubm100", "root", "");
			Statement s = connection.createStatement();
			for (String file : files) {
				String query = queries.get(file);
				long t1 = System.currentTimeMillis();
				ResultSet rs = s.executeQuery(query);
				int results = 0;
				while (rs.next()) {
					// Object a=rs.getObject(1);
					results++;
				}
				rs.close();
				System.out.println(
						"Query " + file + " executed in Mysql in " + (System.currentTimeMillis() - t1) + " ms ");
				System.out.println("No of results:" + results);
			}
			connection.close();
		}
		if (sqlite.equals("1")) {
			dir = "/home/dimitris/sqlitenpd";
			queries.clear();
			queries = new HashMap<String, String>();
			files = readFilesFromDir(dir, endsWith);
			for (String file : files) {
				queries.put(file, readFile(file));
			}
			Class.forName("org.sqlite.JDBC");
			org.sqlite.SQLiteConfig config = new org.sqlite.SQLiteConfig();
			// config.setCacheSize(1200000);
			// config.setPageSize(4096);
			// config.setLockingMode(mode);
			SQLiteConnectionPoolDataSource dataSource = new SQLiteConnectionPoolDataSource();
			dataSource.setUrl("jdbc:sqlite:/media/dimitris/T/exaremenpd" + scale + "/test2.db");
			dataSource.setConfig(config);
			Connection connection = dataSource.getConnection();// DriverManager.getConnection("jdbc:sqlite:test.db");

			// connection.createStatement().execute("PRAGMA journal_mode =
			// OFF");
			connection.createStatement().execute("PRAGMA synchronous = OFF");
			connection.createStatement().execute("PRAGMA ignore_check_constraints = true;");
			connection.createStatement().execute("PRAGMA locking_mode = EXCLUSIVE");
			connection.createStatement().execute("PRAGMA automatic_index = TRUE");
			connection.createStatement().execute("PRAGMA page_size = 16384");
			connection.createStatement().execute("PRAGMA cache_size = 600000");

			Statement s = connection.createStatement();
			// s.execute("PRAGMA cache_size = 600000");
			s.execute("attach database '/media/dimitris/T/exaremenpd" + scale + "/company.0.db' as company");
			s.execute("attach database '/media/dimitris/T/exaremenpd" + scale
					+ "/strat_litho_wellbore_core.0.db' as strat_litho_wellbore_core");
			s.execute(
					"attach database '/media/dimitris/T/exaremenpd" + scale + "/wellbore_core.0.db' as wellbore_core");
			s.execute("attach database '/media/dimitris/T/exaremenpd" + scale
					+ "/wellbore_development_all.0.db' as wellbore_development_all");
			s.execute("attach database '/media/dimitris/T/exaremenpd" + scale
					+ "/wellbore_exploration_all.0.db' as wellbore_exploration_all");
			s.execute("attach database '/media/dimitris/T/exaremenpd" + scale
					+ "/wellbore_npdid_overview.0.db' as wellbore_npdid_overview");
			s.execute("attach database '/media/dimitris/T/exaremenpd" + scale
					+ "/wellbore_shallow_all.0.db' as wellbore_shallow_all");
			s.execute("attach database '/media/dimitris/T/exaremenpd" + scale + "/discovery.0.db' as discovery");
			s.execute("attach database '/media/dimitris/T/exaremenpd" + scale + "/field.0.db' as field");

			for (String file : queries.keySet()) {
				String query = queries.get(file);
				try {
					long t1 = System.currentTimeMillis();
					ResultSet rs = s.executeQuery(query);
					int results = 0;
					while (rs.next()) {
						// System.out.println(rs.getObject(4));
						results++;
					}
					rs.close();
					System.out.println(
							"Query " + file + " executed in sqlite in " + (System.currentTimeMillis() - t1) + " ms ");
					System.out.println("No of results:" + results);
					out.append(
							"Query " + file + " executed in sqlite in " + (System.currentTimeMillis() - t1) + " ms ");
					out.append("No of results:" + results);
				} catch (Exception e) {
					System.out.println("error in query " + file);
					System.out.println(e.getMessage());
					out.append("error in query " + file);
					out.append(e.getMessage());
				}
			}
			connection.close();
		}

		writeFile(outfile, out.toString());

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

	private static String[] readFilesFromDir(String string, String endsWith) throws IOException {
		File folder = new File(string);
		File[] listOfFiles = folder.listFiles();
		List<String> files = new ArrayList<String>();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile() && listOfFiles[i].getCanonicalPath().endsWith(endsWith)) {
				// if(listOfFiles[i].getCanonicalPath().endsWith("30.q.sql"))
				// continue;
				if (listOfFiles[i].getCanonicalPath().endsWith("lubm2.q.sql"))
					continue;
				files.add(listOfFiles[i].getCanonicalPath());
			}
		}
		java.util.Collections.sort(files);
		return files.toArray(new String[files.size()]);
	}

	public static void writeFile(String filename, String string) {
		writeFile(filename, string.getBytes());
	}

	public static void writeFile(String filename, byte[] string) {
		try {
			File file = new File(filename);

			OutputStream out = new FileOutputStream(file);
			out.write(string);
			out.close();
		} catch (Exception e) {
			System.err.println("Error writing file: " + filename);
			e.printStackTrace();
		}
	}
}
