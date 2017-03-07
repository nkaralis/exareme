package madgik.exareme.master.gateway.async.handler;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import madgik.exareme.master.client.AdpDBClient;
import madgik.exareme.master.client.AdpDBClientFactory;
import madgik.exareme.master.client.AdpDBClientProperties;
import madgik.exareme.master.client.AdpDBClientQueryStatus;
import madgik.exareme.master.connector.AdpDBConnector;
import madgik.exareme.master.connector.AdpDBConnectorFactory;
import madgik.exareme.master.connector.local.AdpDBQueryExecutorThread;
import madgik.exareme.master.connector.rmi.AdpDBNetReaderThread;
import madgik.exareme.master.engine.AdpDBManager;
import madgik.exareme.master.engine.AdpDBManagerLocator;
import madgik.exareme.master.engine.executor.FinalUnionExecutor;
import madgik.exareme.master.engine.executor.ResultBuffer;
import madgik.exareme.master.engine.executor.SQLiteLocalExecutor;
import madgik.exareme.master.gateway.ExaremeGatewayUtils;
import madgik.exareme.master.queryProcessor.analyzer.fanalyzer.OptiqueAnalyzer;
import madgik.exareme.master.queryProcessor.analyzer.stat.StatUtils;
import madgik.exareme.master.queryProcessor.decomposer.DecomposerUtils;
import madgik.exareme.master.queryProcessor.decomposer.ViewInfo;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.federation.DB;
import madgik.exareme.master.queryProcessor.decomposer.federation.DBInfoReaderDB;
import madgik.exareme.master.queryProcessor.decomposer.federation.DBInfoWriterDB;
import madgik.exareme.master.queryProcessor.decomposer.federation.DataImporter;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.Operand;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.queryProcessor.decomposer.query.Table;
import madgik.exareme.master.queryProcessor.decomposer.util.InterfaceAdapter;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import madgik.exareme.master.queryProcessor.estimator.db.Schema;
import madgik.exareme.worker.art.registry.ArtRegistryLocator;

import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.protocol.*;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.sqlite.SQLiteOpenMode;
import org.sqlite.javax.SQLiteConnectionPoolDataSource;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Exareme Decomposer Handler.
 *
 * @author alex
 * @author Dimitris
 * @since 0.1
 */
public class HttpAsyncDecomposerHandler implements HttpAsyncRequestHandler<HttpRequest> {

	private static final Logger log = Logger.getLogger(HttpAsyncDecomposerHandler.class);
	private static final AdpDBManager manager = AdpDBManagerLocator.getDBManager();
	private static NamesToAliases n2a=new NamesToAliases();
	public HttpAsyncDecomposerHandler() {
	}

	@Override
	public HttpAsyncRequestConsumer<HttpRequest> processRequest(final HttpRequest request, final HttpContext context)
			throws HttpException, IOException {
		return new BasicAsyncRequestConsumer();
	}

	@Override
	public void handle(final HttpRequest httpRequest, final HttpAsyncExchange httpExchange, final HttpContext context)
			throws HttpException, IOException {
		final HttpResponse httpResponse = httpExchange.getResponse();
		log.trace("Parsing request ...");
		String method = httpRequest.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH);
		if (!"GET".equals(method) && !"POST".equals(method))
			throw new UnsupportedHttpVersionException(method + "not supported.");
		String content = "";
		if (httpRequest instanceof HttpEntityEnclosingRequest) {
			log.debug("Stream ...");
			HttpEntity entity = ((HttpEntityEnclosingRequest) httpRequest).getEntity();
			content = EntityUtils.toString(entity);
		}

		final HashMap<String, String> inputContent = new HashMap<String, String>();
		ExaremeGatewayUtils.getValues(content, inputContent);

		final String dbname = inputContent.get(ExaremeGatewayUtils.REQUEST_DATABASE);
		final String query = inputContent.get(ExaremeGatewayUtils.REQUEST_QUERY);
		final int workers = ArtRegistryLocator.getArtRegistryProxy().getContainers().length;
		
		final boolean local=false;

		log.debug("--DB " + dbname);
		log.debug("--Query " + query);

		log.trace("Decomposing ...");
		new Thread() {
			@Override
			public void run() {
				try {
					if(query.startsWith("sparql")){
						executeLocal(null, null);
					}
					else if (query.startsWith("addFederatedEndpoint")) {

						log.debug("Adding endpoint to : " + dbname + "endpoint.db ...");
						DBInfoWriterDB.write(query, dbname);
						InputStreamEntity se = new InputStreamEntity(createOKResultStream(), -1,
								ContentType.TEXT_PLAIN);
						log.debug("Sending OK : " + se.toString());
						httpResponse.setEntity(se);
					} else if (query.startsWith("getTables")) {
						Class.forName("org.sqlite.JDBC");
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}

						Connection c = createDummyDB(path);

						// Connection c = cons.get(path);
						// Connection c = DriverManager
						// .getConnection("jdbc:sqlite:" + path+"registry.db");
						String[] params = query.split(" ");
						for (int i = 1; i < params.length; i++) {
							if (params[i].equalsIgnoreCase("null")) {
								params[i] = null;
							}
						}
						ResultSet first = c.getMetaData().getTables(params[1], params[2], params[3], null);

						ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
						for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
							ArrayList<String> nextCouple = new ArrayList<String>();
							nextCouple.add(first.getMetaData().getColumnName(i).toUpperCase());
							nextCouple.add(first.getMetaData().getColumnTypeName(i));
							schema.add(nextCouple);
						}
						/*
						 * ArrayList<String> typenames=new ArrayList<String>();
						 * ArrayList<String> names=new ArrayList<String>();
						 * for(int
						 * i=1;i<first.getMetaData().getColumnCount()+1;i++){
						 * names.add(first.getMetaData().getColumnName(i));
						 * typenames
						 * .add(first.getMetaData().getColumnTypeName(i)); }
						 * schema.add(names); schema.add(typenames);
						 */

						HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
						h.put("schema", schema);
						h.put("errors", new ArrayList<ArrayList<String>>());
						Gson g = new Gson();
						StringBuilder sb = new StringBuilder();
						sb.append(g.toJson(h, h.getClass()));
						while (first.next()) {
							if (first.getString(3).startsWith("table")) {
								// ignore temporary tables
								continue;
							}
							sb.append("\n");
							ArrayList<Object> res = new ArrayList<Object>();
							for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
								res.add(first.getObject(i));

							}
							sb.append(g.toJson((ArrayList<Object>) res));

						}
						first.close();
						// stmt.close();
						c.close();
						log.error("Setting response for Tables:" + sb.toString());
						InputStreamEntity se = new InputStreamEntity(new ByteArrayInputStream(sb.toString().getBytes()),
								-1, ContentType.TEXT_PLAIN);
						httpResponse.setEntity(se);
					} else if (query.startsWith("getIndexInfo")) {
						Class.forName("org.sqlite.JDBC");
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}

						Connection c = createDummyDB(path);

						// Connection c = cons.get(path);
						// Connection c = DriverManager
						// .getConnection("jdbc:sqlite:" + path+"registry.db");
						String[] params = query.split(" ");
						for (int i = 1; i < params.length; i++) {
							if (params[i].equalsIgnoreCase("null")) {
								params[i] = null;
							}
						}
						ResultSet first = c.getMetaData().getIndexInfo(params[1], null, params[3],
								Boolean.parseBoolean(params[4]), Boolean.parseBoolean(params[5]));

						ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
						for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
							ArrayList<String> nextCouple = new ArrayList<String>();
							nextCouple.add(first.getMetaData().getColumnName(i).toUpperCase());
							nextCouple.add(first.getMetaData().getColumnTypeName(i));
							schema.add(nextCouple);
						}
						/*
						 * ArrayList<String> typenames=new ArrayList<String>();
						 * ArrayList<String> names=new ArrayList<String>();
						 * for(int
						 * i=1;i<first.getMetaData().getColumnCount()+1;i++){
						 * names.add(first.getMetaData().getColumnName(i));
						 * typenames
						 * .add(first.getMetaData().getColumnTypeName(i)); }
						 * schema.add(names); schema.add(typenames);
						 */

						HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
						h.put("schema", schema);
						h.put("errors", new ArrayList<ArrayList<String>>());
						Gson g = new Gson();
						StringBuilder sb = new StringBuilder();
						sb.append(g.toJson(h, h.getClass()));
						while (first.next()) {
							sb.append("\n");
							ArrayList<Object> res = new ArrayList<Object>();
							for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
								res.add(first.getObject(i));

							}
							sb.append(g.toJson((ArrayList<Object>) res));

						}
						first.close();
						// stmt.close();
						c.close();

						InputStreamEntity se = new InputStreamEntity(new ByteArrayInputStream(sb.toString().getBytes()),
								-1, ContentType.TEXT_PLAIN);
						httpResponse.setEntity(se);
					} else if (query.startsWith("getColumns")) {
						Class.forName("org.sqlite.JDBC");
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}

						Connection c = createDummyDB(path);
						log.debug("con to dummy db:" + c);
						// Connection c = DriverManager
						// .getConnection("jdbc:sqlite:" + path+"registry.db");
						String[] params = query.split(" ");
						for (int i = 1; i < params.length; i++) {
							if (params[i].equalsIgnoreCase("null")) {
								params[i] = null;
							}
						}
						ResultSet first = c.getMetaData().getColumns(params[1], params[2], params[3], params[4]);
						// log.error(params[1]+ params[2]+ params[3]+
						// params[4]);
						ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
						for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
							// log.error(first.getMetaData().getColumnName(i));
							ArrayList<String> nextCouple = new ArrayList<String>();
							nextCouple.add(first.getMetaData().getColumnName(i).toUpperCase());
							nextCouple.add(first.getMetaData().getColumnTypeName(i));
							schema.add(nextCouple);
						}
						/*
						 * ArrayList<String> typenames=new ArrayList<String>();
						 * ArrayList<String> names=new ArrayList<String>();
						 * for(int
						 * i=1;i<first.getMetaData().getColumnCount()+1;i++){
						 * names.add(first.getMetaData().getColumnName(i));
						 * typenames
						 * .add(first.getMetaData().getColumnTypeName(i)); }
						 * schema.add(names); schema.add(typenames);
						 */

						HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
						h.put("schema", schema);
						h.put("errors", new ArrayList<ArrayList<String>>());
						Gson g = new Gson();
						StringBuilder sb = new StringBuilder();
						sb.append(g.toJson(h, h.getClass()));

						while (first.next()) {
							sb.append("\n");
							ArrayList<Object> res = new ArrayList<Object>();
							for (int i = 1; i < first.getMetaData().getColumnCount() + 1; i++) {
								// log.error(first.getObject(i));
								res.add(first.getObject(i));

							}
							sb.append(g.toJson((ArrayList<Object>) res));

						}
						first.close();
						// stmt.close();
						c.close();
						// log.error("Setting response for Coluns:" +
						// sb.toString());
						InputStreamEntity se = new InputStreamEntity(new ByteArrayInputStream(sb.toString().getBytes()),
								-1, ContentType.TEXT_PLAIN);
						httpResponse.setEntity(se);
					} else if (query.startsWith("explain") || query.startsWith("EXPLAIN")) {
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}
						NodeSelectivityEstimator nse = null;
						try {
							nse = new NodeSelectivityEstimator(path + "histograms.json");
						} catch (Exception e) {
							log.error("Cannot read statistics. " + e.getMessage());
						}
						List<SQLQuery> subqueries = new ArrayList<SQLQuery>();
						SQLQuery squery;
						try {
							log.debug("Parsing SQL Query ...");
							NodeHashValues hashes=new NodeHashValues();
							hashes.setSelectivityEstimator(nse);
							squery = SQLQueryParser.parse(query.substring(8, query.length()), hashes);
							QueryDecomposer d = new QueryDecomposer(squery, path, workers, hashes);
							d.setN2a(n2a);
							log.debug("SQL Query Decomposing ...");
							log.debug("Number of workers: " + workers);
							d.setImportExternal(false);
							subqueries = d.getSubqueries();
							nse=null;
							hashes=null;
							ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
							// ArrayList<String> typenames=new
							// ArrayList<String>();
							// typenames.add("VARCHAR");
							// schema.add(typenames);
							ArrayList<String> names = new ArrayList<String>();
							names.add("RESULT");
							names.add("VARCHAR");
							schema.add(names);
							HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
							h.put("schema", schema);
							h.put("errors", new ArrayList<ArrayList<String>>());
							Gson g = new Gson();
							StringBuilder sb = new StringBuilder();
							sb.append(g.toJson(h, h.getClass()));

							HashMap<String, byte[]> hashIDMap = new HashMap<>();
							for (SQLQuery q : subqueries) {
								hashIDMap.put(q.getResultTableName(), q.getHashId().asBytes());
								sb.append("\n");
								ArrayList<Object> res = new ArrayList<Object>();
								if (q.isFederated()) {
									q.removePasswordFromMadis();
								}
								res.add(q.toDistSQL());
								sb.append(g.toJson((ArrayList<Object>) res));
							}
							httpResponse.setEntity(
									new InputStreamEntity(new ByteArrayInputStream(sb.toString().getBytes())));
						} catch (Exception e) {
							log.error(e);
							httpResponse.setStatusCode(500);
							if (e.getMessage() == null) {
								httpResponse
										.setEntity(new StringEntity("error explaining query", ContentType.TEXT_PLAIN));
							} else {
								httpResponse.setEntity(new StringEntity(e.getMessage(), ContentType.TEXT_PLAIN));
							}

						}

					} else if (query.startsWith("analyzeTable")) {
						String[] t = query.replace("analyzeTable ", "").split(" ");
						if (t.length == 0) {
							log.warn("Cannot analyze table, no columns given");
							InputStreamEntity se = new InputStreamEntity(createOKResultStream(), -1,
									ContentType.TEXT_PLAIN);

							httpResponse.setEntity(se);
						} else {
							try {
								String path = dbname;
								if (!path.endsWith("/")) {
									path += "/";
								}

								DBInfoReaderDB.read(path);
								String tablename = t[0];
								Table tab = new Table(tablename, tablename);
								String endpointID = tab.getDBName();
								String localTblName = tablename.replace(endpointID + "_", "");
								DB db = DBInfoReaderDB.dbInfo.getDB(endpointID);

								Set<String> attrs = new HashSet<String>();
								for (int i = 1; i < t.length; i++) {
									attrs.add(t[i]);
								}

								OptiqueAnalyzer fa = new OptiqueAnalyzer(dbname, db);
								Schema sch = fa.analyzeAttrs(localTblName, attrs);
								// change table name back to adding DB id
								sch.getTableIndex().put(tablename, sch.getTableIndex().get(localTblName));
								sch.getTableIndex().remove(localTblName);
								StatUtils.addSchemaToFile(path + "histograms.json", sch);
								InputStreamEntity se = new InputStreamEntity(createOKResultStream(), -1,
										ContentType.TEXT_PLAIN);
								log.debug("Sending OK : " + se.toString());
								httpResponse.setEntity(se);
							} catch (Exception e) {
								log.error(e);
								httpResponse.setStatusCode(500);
								httpResponse.setEntity(new StringEntity(e.getMessage(), ContentType.TEXT_PLAIN));
							}
						}
					} else if (query.equals("select 1")) {

						// temporary fix to avoid executing onto select 1
						// queries
						InputStreamEntity se = new InputStreamEntity(create1ResultStream(), -1, ContentType.TEXT_PLAIN);
						log.debug("Sending 1 : " + se.toString());
						httpResponse.setEntity(se);
					} else if (!query.startsWith("distributed")) {
						//long timestart=System.currentTimeMillis();
						int timeoutMs = 0;
						try {
							timeoutMs = Integer.parseInt(inputContent.get(ExaremeGatewayUtils.REQUEST_TIMEOUT)) * 1000;
						} catch (NumberFormatException e) {
							log.error("Timeout not an integer:" + ExaremeGatewayUtils.REQUEST_TIMEOUT);
							log.error(inputContent.toString());
						}
						long start = System.currentTimeMillis();
						String path = dbname;
						if (!path.endsWith("/")) {
							path += "/";
						}
						NodeSelectivityEstimator nse = null;
						Map<Set<String>, Set<ViewInfo>> viewinfos = new HashMap<Set<String>, Set<ViewInfo>>();
						try {
							nse = new NodeSelectivityEstimator(path + "histograms.json");
							BufferedReader br;
							br = new BufferedReader(new FileReader("/media/dimitris/T/exaremelubm100/" + "views.json"));

							// convert the json string back to object
							// Gson gson = new Gson();
							Gson gson = new GsonBuilder().registerTypeAdapter(Operand.class, new InterfaceAdapter<Operand>()).create();
							java.lang.reflect.Type viewType = new TypeToken<Map<Set<String>, Set<ViewInfo>>>() {
							}.getType();
							viewinfos = gson.fromJson(br, viewType);
						} catch (Exception e) {
							log.error("Cannot read statistics. " + e.getMessage());
						}
						
						
						
						List<SQLQuery> subqueries = new ArrayList<SQLQuery>();
						SQLQuery squery;
						try {
							log.debug("Parsing SQL Query ...");
							NodeHashValues hashes=new NodeHashValues();
							hashes.setSelectivityEstimator(nse);
							squery = SQLQueryParser.parse(query, hashes);
							QueryDecomposer d = new QueryDecomposer(squery, path, workers, hashes);
							d.setViewInfos(viewinfos);
							if(DecomposerUtils.WRITE_ALIASES){
							n2a=DBInfoReaderDB.readAliases(path);}
							d.setN2a(n2a);
							log.debug("n2a:"+n2a.toString());
							log.debug("SQL Query Decomposing ...");
							log.debug("Number of workers: " + workers);
							subqueries = d.getSubqueries();
							if(local){
								executeLocal(subqueries, path);
								return;
							}
							String decomposedQuery = "";
							String resultTblName = "";
							if(DecomposerUtils.WRITE_ALIASES){
							DBInfoWriterDB.writeAliases(n2a, path);
							}
							squery=null;
							d=null;
							nse=null;
							hashes=null;
							AdpDBClientProperties props = new AdpDBClientProperties(dbname, "", "", false, false, false,
									-1, 10, null);
							AdpDBClient dbClient = AdpDBClientFactory.createDBClient(manager, props);
							Set<String> referencedTables=new HashSet<String>();
							boolean a=false;
							
							String finalQuery = null;
							if(a){
								SQLQuery last=subqueries.get(subqueries.size()-1);
								finalQuery=last.toSQL();
								for(Table t:last.getAllAttachedTables()){
									referencedTables.add(t.getName());
								}
								
							}
							if (subqueries.size() == 1 && subqueries.get(0).existsInCache()) {
								resultTblName = subqueries.get(0).getInputTables().get(0).getAlias();
							} else {
								HashMap<String, byte[]> hashIDMap = new HashMap<>();
								Map<String, String> extraCommands=new HashMap<String, String>();
								for (SQLQuery q : subqueries) {
									//when using cache
									//hashIDMap.put(q.getResultTableName(), q.getHashId().asBytes());
									if(a && !q.isTemporary()){
										continue;
										//don't add last table
									}
									if(referencedTables.contains(q.getTemporaryTableName())){
										q.setTemporary(false);
									}
									
									String dSQL = q.toDistSQL();
									decomposedQuery += dSQL + "\n\n";
									if (!q.isTemporary()) {
										resultTblName = q.getTemporaryTableName();
									}
									if(q.getCreateSipTables()!=null){
										extraCommands.put(q.getTemporaryTableName(), q.getCreateSipTables());
									}
									
								}
								log.debug("Decomposed Query : " + decomposedQuery);
								log.debug("Result Table : " + resultTblName);
								log.debug("Executing decomposed query ...");
								
								//when using cache
								//AdpDBClientQueryStatus status = dbClient.query("dquery", decomposedQuery, hashIDMap);
								//System.out.println("DECOMPOSER TIME:"+(System.currentTimeMillis()-timestart));
								AdpDBClientQueryStatus status = dbClient.query("dquery", decomposedQuery, extraCommands);
								while (status.hasFinished() == false && status.hasError() == false) {
									if (timeoutMs > 0) {
										long timePassed = System.currentTimeMillis() - start;
										if (timePassed > timeoutMs) {
											status.close();
											log.warn("Time out:" + timeoutMs + " ms passed");
											throw new RuntimeException("Time out:" + timeoutMs + " ms passed");
										}
									}
									Thread.sleep(50);
								}
								if (status.hasError()) {
									throw new RuntimeException(status.getError());
								}
							}
							
							if(a){
								HashMap<String, Object> additionalProps = new HashMap<String, Object>();
						        additionalProps.put("time", -1);
						        additionalProps.put("errors", new ArrayList<Object>());
								ExecutorService pool = Executors.newFixedThreadPool(1);
								PipedOutputStream out = new PipedOutputStream();
					            pool.submit(new AdpDBQueryExecutorThread(finalQuery, additionalProps, props, referencedTables, out));
					            log.debug("Net Reader submitted.");
							log.debug("Sending Result Table : " + resultTblName);
						        httpResponse.setEntity(new InputStreamEntity(new PipedInputStream(out)));
							}
							else{
							log.debug("Sending Result Table : " + resultTblName);
							httpResponse.setEntity(new InputStreamEntity(dbClient.readTable(resultTblName)));
							}
						} catch (Exception e) {
							log.error("Error:", e);
							httpResponse.setStatusCode(500);
							if (e.getMessage() == null) {
								httpResponse
										.setEntity(new StringEntity("error executing query", ContentType.TEXT_PLAIN));
							} else {
								httpResponse.setEntity(new StringEntity(e.getMessage(), ContentType.TEXT_PLAIN));
							}

						}
					} else {
						log.error("Bad request");
						throw new RuntimeException("Bad Request");
					}
				} catch (Exception e) {
					// e.printStackTrace();
					log.error(e);
					httpResponse.setStatusCode(500);
					httpResponse.setEntity(new StringEntity(e.getMessage(), ContentType.TEXT_PLAIN));
				}
				httpExchange.submitResponse(new BasicAsyncResponseProducer(httpResponse));
			}

private void executeLocal(List<SQLQuery> subqueries, String path) throws ClassNotFoundException, SQLException, InterruptedException {
				
	
				
	subqueries=new ArrayList<SQLQuery>();
	for(int t=1;t<17;t++){
				SQLQuery a=new SQLQuery();
				a.setStringSQL();
				a.setSQL("select * from "
									+" R"+t+" st cross join w where st.o=w.s;\n");
				subqueries.add(a);
				 
	}
			     long start=System.currentTimeMillis();
				ExecutorService es = Executors.newFixedThreadPool(16);
				Map<String, SQLQuery> tableNames=new HashMap<String, SQLQuery>();
				for(int i=0;i<subqueries.size()-1;i++){
					SQLQuery next=subqueries.get(i);
					tableNames.put(next.getTemporaryTableName(), next);
					
				}
				//Connection ccc=getConnection("");
				List<SQLiteLocalExecutor> executors=new ArrayList<SQLiteLocalExecutor>();
				ResultBuffer globalBuffer=new ResultBuffer();
				Set<SQLQuery> finishedQueries=new HashSet<SQLQuery>();
				for (int i = 0; i < subqueries.size()-1; i++) {
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
				FinalUnionExecutor ex=new FinalUnionExecutor(globalBuffer, null,16);
				es.execute(ex);
				for(SQLiteLocalExecutor exec:executors){
					es.execute(exec);
				}
				es.shutdown();
				boolean finished = es.awaitTermination(300, TimeUnit.MINUTES);
				
			//	if(finished){
			//		c2.commit();
			//	connection.commit();
				
				System.out.println(System.currentTimeMillis()-start);//}
}

			
				
			
		}.start();
	}

	private InputStream createOKResultStream() {
		// HashMap<String, ArrayList<ArrayList<String>>> firstRow=new
		// HashMap<String, ArrayList<ArrayList<String>>>();
		ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
		// ArrayList<String> typenames=new ArrayList<String>();
		// typenames.add("VARCHAR");
		// schema.add(typenames);
		ArrayList<String> names = new ArrayList<String>();
		names.add("RESULT");
		names.add("VARCHAR");
		schema.add(names);
		HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
		h.put("schema", schema);
		h.put("errors", new ArrayList<ArrayList<String>>());
		Gson g = new Gson();
		StringBuilder sb = new StringBuilder();
		sb.append(g.toJson(h, h.getClass()));
		sb.append("\n");
		ArrayList<Object> res = new ArrayList<Object>();
		res.add("OK");
		sb.append(g.toJson((ArrayList<Object>) res));
		return new ByteArrayInputStream(sb.toString().getBytes());
	}

	private InputStream create1ResultStream() {
		// HashMap<String, ArrayList<ArrayList<String>>> firstRow=new
		// HashMap<String, ArrayList<ArrayList<String>>>();
		ArrayList<ArrayList<String>> schema = new ArrayList<ArrayList<String>>();
		// ArrayList<String> typenames=new ArrayList<String>();
		// typenames.add("VARCHAR");
		// schema.add(typenames);
		ArrayList<String> names = new ArrayList<String>();
		names.add("RESULT");
		names.add("INTEGER");
		schema.add(names);
		HashMap<String, ArrayList<ArrayList<String>>> h = new HashMap<String, ArrayList<ArrayList<String>>>();
		h.put("schema", schema);
		h.put("errors", new ArrayList<ArrayList<String>>());
		Gson g = new Gson();
		StringBuilder sb = new StringBuilder();
		sb.append(g.toJson(h, h.getClass()));
		sb.append("\n");
		ArrayList<Object> res = new ArrayList<Object>();
		res.add(new Integer(1));
		sb.append(g.toJson((ArrayList<Object>) res));
		return new ByteArrayInputStream(sb.toString().getBytes());
	}

	private Connection createDummyDB(String path) {
		try {
			Class.forName("org.sqlite.JDBC");
			Connection c = DriverManager.getConnection("jdbc:sqlite:" + path + "registry.db");
			Statement stmt = c.createStatement();
			ResultSet rs;
			Connection c2 = DriverManager.getConnection("jdbc:sqlite:" + path + "dummy.db");
			try {
				rs = stmt.executeQuery("SELECT sql_definition FROM sql");
			} catch (Exception e) {
				log.debug("Could not read registry");
				stmt.close();
				c.close();
				return c2;
			}
			log.debug("getting sql definition");

			Statement statement = c2.createStatement();
			String create = rs.getString(1);
			create = create.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
			while (rs.next()) {
				create = rs.getString(1);
				create = create.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
				statement.executeUpdate(create);
			}
			rs.close();
			statement.close();
			stmt.close();
			c.close();
			return c2;
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e);
		}
		return null;
	}
	
	private Connection getConnection(String postfix){
		try{
		Class.forName("org.sqlite.JDBC");
		 org.sqlite.SQLiteConfig config = new org.sqlite.SQLiteConfig();
		 config.setSharedCache(true);
	config.enableLoadExtension(true);
		// config.setOpenMode(SQLiteOpenMode.READONLY);
		// config.setOpenMode(SQLiteOpenMode.CREATE);
		 config.setOpenMode(SQLiteOpenMode.NOMUTEX);
		 config.setOpenMode(SQLiteOpenMode.SHAREDCACHE);
		 //config.setSharedCache(true);
		 
		 config.setCacheSize(2400000);
		 config.setPageSize(4096);
		 //config.setLockingMode(mode);
		 SQLiteConnectionPoolDataSource dataSource = new SQLiteConnectionPoolDataSource();
		  //dataSource.setUrl("jdbc:sqlite:file:memdb1?mode=memory&nolock=1");
		dataSource.setUrl("jdbc:sqlite:file:/media/dimitris/T/tmp/pxbpwj"+postfix+"?cache=shared&nolock=1");
		 //dataSource.setUrl("jdbc:sqlite::memory:");
		    dataSource.setConfig(config);
		    //=dataSource.getConnection();//
		//Connection connection=DriverManager.getConnection("jdbc:sqlite:file:/media/dimitris/T/tmp/asrrsasasas");
		Connection connection=dataSource.getConnection();
		//outofmemory
		
		
		Statement stmt=connection.createStatement();
		stmt.execute("PRAGMA locking_mode = EXCLUSIVE");
		stmt.execute("PRAGMA threads = 8");
		stmt.execute("PRAGMA auto_vacuum = NONE"); 
		stmt.execute("PRAGMA journal_mode = OFF");
		//stmt.execute("PRAGMA synchronous = OFF");
		stmt.execute("PRAGMA ignore_check_constraints = true");
		stmt.execute("PRAGMA read_uncommitted = true");
		/*stmt.execute("PRAGMA locking_mode = EXCLUSIVE");
		stmt.execute("PRAGMA automatic_index = TRUE");
		
		stmt.execute("PRAGMA page_size = 4096");
		stmt.execute("PRAGMA cache_size = 2400000");*/
		stmt.execute("select load_extension('/home/dimitris/virtualtables/wrapper')");
		//stmt.execute("PRAGMA journal_mode = WAL");
		stmt.execute("PRAGMA read_uncommited = TRUE");
		
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
		stmt.execute("ATTACH 'file:/media/dimitris/T/test/rdf.0.db?immutable=1&cache=private&nolock=1' AS siptable0"); 
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