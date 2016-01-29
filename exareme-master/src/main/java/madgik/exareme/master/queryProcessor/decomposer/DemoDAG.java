package madgik.exareme.master.queryProcessor.decomposer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import madgik.exareme.master.queryProcessor.decomposer.federation.DBInfoReaderDB;
import madgik.exareme.master.queryProcessor.decomposer.federation.NamesToAliases;
import madgik.exareme.master.queryProcessor.decomposer.federation.QueryDecomposer;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;

public class DemoDAG {

	public static final String queryExample="SELECT *\n" +
			"FROM (\n" +
			"SELECT \n" +
			"   7 AS \"wellboreQuestType\", NULL AS \"wellboreLang\", CAST(QVIEW1.\"wlbWellboreName\" AS VARCHAR(10485760)) AS \"wellbore\", \n" +
			"   5 AS \"lenghtMQuestType\", NULL AS \"lenghtMLang\", CAST(QVIEW4.\"lsuCoreLenght\" AS VARCHAR(10485760)) AS \"lenghtM\", \n" +
			"   7 AS \"companyQuestType\", NULL AS \"companyLang\", CAST(QVIEW2.\"wlbDrillingOperator\" AS VARCHAR(10485760)) AS \"company\", \n" +
			"   4 AS \"yearQuestType\", NULL AS \"yearLang\", CAST(QVIEW2.\"wlbCompletionYear\" AS VARCHAR(10485760)) AS \"year\"\n" +
			" FROM \n" +
			"\"wellbore_development_all\" QVIEW1,\n" +
			"\"wellbore_exploration_all\" QVIEW2,\n" +
			"\"company\" QVIEW3,\n" +
			"\"strat_litho_wellbore_core\" QVIEW4,\n" +
			"\"wellbore_npdid_overview\" QVIEW5\n" +
			"WHERE \n" +
			"QVIEW1.\"wlbWellboreName\" IS NOT NULL AND\n" +
			"QVIEW1.\"wlbNpdidWellbore\" IS NOT NULL AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW2.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW2.\"wlbCompletionYear\" IS NOT NULL AND\n" +
			"(QVIEW2.\"wlbDrillingOperator\" = QVIEW3.\"cmpLongName\") AND\n" +
			"QVIEW2.\"wlbDrillingOperator\" IS NOT NULL AND\n" +
			"QVIEW3.\"cmpNpdidCompany\" IS NOT NULL AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW4.\"wlbNpdidWellbore\") AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW5.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW4.\"lsuNpdidLithoStrat\" IS NOT NULL AND\n" +
			"QVIEW4.\"lsuCoreLenght\" IS NOT NULL AND\n" +
			"((QVIEW4.\"lsuCoreLenght\" > 50) AND (QVIEW2.\"wlbCompletionYear\" >= 2008))\n" +
			"UNION\n" +
			"SELECT \n" +
			"   7 AS \"wellboreQuestType\", NULL AS \"wellboreLang\", CAST(QVIEW1.\"wlbWellboreName\" AS VARCHAR(10485760)) AS \"wellbore\", \n" +
			"   5 AS \"lenghtMQuestType\", NULL AS \"lenghtMLang\", CAST(QVIEW6.\"wlbTotalCoreLength\" AS VARCHAR(10485760)) AS \"lenghtM\", \n" +
			"   7 AS \"companyQuestType\", NULL AS \"companyLang\", CAST(QVIEW2.\"wlbDrillingOperator\" AS VARCHAR(10485760)) AS \"company\", \n" +
			"   4 AS \"yearQuestType\", NULL AS \"yearLang\", CAST(QVIEW2.\"wlbCompletionYear\" AS VARCHAR(10485760)) AS \"year\"\n" +
			" FROM \n" +
			"\"wellbore_development_all\" QVIEW1,\n" +
			"\"wellbore_exploration_all\" QVIEW2,\n" +
			"\"company\" QVIEW3,\n" +
			"\"wellbore_core\" QVIEW4,\n" +
			"\"wellbore_npdid_overview\" QVIEW5,\n" +
			"\"wellbore_core\" QVIEW6\n" +
			"WHERE \n" +
			"QVIEW1.\"wlbWellboreName\" IS NOT NULL AND\n" +
			"QVIEW1.\"wlbNpdidWellbore\" IS NOT NULL AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW2.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW2.\"wlbCompletionYear\" IS NOT NULL AND\n" +
			"(QVIEW2.\"wlbDrillingOperator\" = QVIEW3.\"cmpLongName\") AND\n" +
			"QVIEW2.\"wlbDrillingOperator\" IS NOT NULL AND\n" +
			"QVIEW3.\"cmpNpdidCompany\" IS NOT NULL AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW4.\"wlbNpdidWellbore\") AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW5.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW4.\"wlbCoreNumber\" IS NOT NULL AND\n" +
			"(QVIEW4.\"wlbCoreNumber\" = QVIEW6.\"wlbCoreNumber\") AND\n" +
			"(QVIEW1.\"wlbNpdidWellbore\" = QVIEW6.\"wlbNpdidWellbore\") AND\n" +
			"QVIEW6.\"wlbTotalCoreLength\" IS NOT NULL AND\n" +
			"((QVIEW6.\"wlbTotalCoreLength\" > 50) AND (QVIEW2.\"wlbCompletionYear\" >= 2008))) SUB";
	
	public static void main(String[] args) throws Exception {
		String random=(generateRandomQuery(5, 2));
		//String file = readFile("/home/dimitris/join6.txt");
		String example="Select A.id from A A, B B, C C, D D where A.id=B.id and B.n=C.n and B.id=D.id";
		SQLQuery query = SQLQueryParser.parse(example);
		QueryDecomposer d = new QueryDecomposer(query, "/tmp/", 128, null);
		
		d.setN2a(new NamesToAliases());
		
		for (SQLQuery s : d.getSubqueries()) {
			System.out.println(s.getHashId()+" : \n"+s.toDistSQL());
		}
		

	}
	
	public static String generateRandomQuery(int noOfTables, int noOfAttributes){
		if(noOfTables<2){
			return "";
		}
		List<HashSet<Integer>> connectedSets=new ArrayList<HashSet<Integer>>();
		StringBuilder sb=new StringBuilder();
		sb.append("Select temp1.att1 from ");
		String delimiter="";
		for(int i=1;i<noOfTables+1;i++){
			HashSet<Integer> hs=new HashSet<Integer>();
			hs.add(i);
			connectedSets.add(hs);
			sb.append(delimiter);
			sb.append("temp");
			sb.append(i);
			delimiter=", ";
		}
		sb.append(" where ");
		delimiter="";
		for(int i=1;i<noOfTables;i++){
			sb.append(delimiter);
			sb.append("temp");
			sb.append(i);
			sb.append(".att");
			sb.append(getRandomInt(noOfAttributes));
			sb.append("=temp");
			HashSet<Integer> conset=null;
			for(HashSet<Integer> next:connectedSets){
				if(next.contains(i)){
					conset=next;
					break;
				}
			}
			int otherTable=getRandomInt(noOfTables, conset);
			HashSet<Integer> otherset=null;
			for(HashSet<Integer> next:connectedSets){
				if(next.contains(otherTable)){
					otherset=next;
					break;
				}
			}
			for(Integer j:otherset){
				conset.add(j);
			}
			connectedSets.remove(otherset);
			sb.append(otherTable);
			sb.append(".att");
			sb.append(getRandomInt(noOfAttributes));
			delimiter=" and ";
		}
		
		
		return sb.toString();
	}
	
	public static int getRandomInt(int max){
		return ThreadLocalRandom.current().nextInt(1, max+1);
	}
	public static int getRandomInt(int max,HashSet<Integer> exclude){
		int result=ThreadLocalRandom.current().nextInt(1, max+1);
		while(exclude.contains(result)){
			result=ThreadLocalRandom.current().nextInt(1, max+1);
		}
		
		return result;
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

}
