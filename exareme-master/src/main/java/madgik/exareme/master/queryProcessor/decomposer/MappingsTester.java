package madgik.exareme.master.queryProcessor.decomposer;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyDomainAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyRangeAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import it.unibz.krdb.obda.model.Function;
import it.unibz.krdb.obda.model.OBDAMappingAxiom;
import it.unibz.krdb.obda.model.OBDASQLQuery;
import it.unibz.krdb.obda.model.Predicate;
import it.unibz.krdb.obda.model.Term;
import it.unibz.krdb.obda.model.impl.FunctionalTermImpl;
import it.unibz.krdb.obda.model.impl.SQLQueryImpl;
import it.unibz.krdb.obda.model.impl.VariableImpl;
import it.unibz.krdb.obda.r2rml.R2RMLReader;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;

public class MappingsTester {

	public static void main(String[] args) {
		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = null;
		try {
			ontology = manager.loadOntologyFromOntologyDocument(new File("/home/dimitris/ontopv1/iswc2014-benchmark/LUBM/univ-benchQL.owl"));
		} catch (OWLOntologyCreationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Set<String> domains=new HashSet<String>();
		Set<String> ranges=new HashSet<String>();
		for(OWLObjectProperty op:ontology.getObjectPropertiesInSignature()){
			for(OWLObjectPropertyDomainAxiom domain:ontology.getObjectPropertyDomainAxioms(op)){
				if(!domain.getClassesInSignature().isEmpty()){
					domains.add(op.getIRI().toString());
					break;
				}
			}
			for(OWLObjectPropertyRangeAxiom range:ontology.getObjectPropertyRangeAxioms(op)){
				if(!range.getClassesInSignature().isEmpty()){
					ranges.add(op.getIRI().toString());
					break;
				}
			}
		}
		for(OWLDataProperty op:ontology.getDataPropertiesInSignature()){
			for(OWLDataPropertyDomainAxiom domain:ontology.getDataPropertyDomainAxioms(op)){
				if(!domain.getClassesInSignature().isEmpty()){
					domains.add(op.getIRI().toString());
					break;
				}
			}
		}
		
		String obdafile="/home/dimitris/ontopv1/iswc2014-benchmark/LUBM/univ-benchQL.ttl";
		R2RMLReader reader=null;
		try {
			reader = new R2RMLReader(obdafile);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 List<OBDAMappingAxiom> ax=reader.readMappings();
		 Map<String, Set<Set<String>>> queryOutputs=new HashMap<String, Set<Set<String>>>();
		 for(OBDAMappingAxiom axiom:ax){
			 String s=null;
			 OBDASQLQuery sql=axiom.getSourceQuery();
			 if(sql instanceof SQLQueryImpl){
				 SQLQueryImpl sqlImpl=(SQLQueryImpl) sql;
				 s=sqlImpl.getSQLQuery();				 
			 }
			 for(Function f:axiom.getTargetQuery()){
				 Predicate a=f.getFunctionSymbol();
				 if(domains.contains(a.getName())){
					Term t=f.getTerm(0); 
					Set<String> outputs=new HashSet<String>();
					if(t instanceof FunctionalTermImpl){
						FunctionalTermImpl fti=(FunctionalTermImpl)t;
						for(int i=1;i<fti.getTerms().size();i++){
							Term term=fti.getTerm(i);
							if(term instanceof VariableImpl){
								VariableImpl v=(VariableImpl)term;
								outputs.add(v.getName());
							}
						}
					}
					if(queryOutputs.containsKey(s)){
						queryOutputs.get(s).add(outputs);
					}
					else{
						Set<Set<String>> outputsForquery=new HashSet<Set<String>>();
						outputsForquery.add(outputs);
						queryOutputs.put(s, outputsForquery);
					}
					
				 }
				 
				 if(ranges.contains(a.getName())){
						Term t=f.getTerm(1); 
						Set<String> outputs=new HashSet<String>();
						if(t instanceof FunctionalTermImpl){
							FunctionalTermImpl fti=(FunctionalTermImpl)t;
							for(int i=1;i<fti.getTerms().size();i++){
								Term term=fti.getTerm(i);
								if(term instanceof VariableImpl){
									VariableImpl v=(VariableImpl)term;
									outputs.add(v.getName());
								}
							}
						}
						if(queryOutputs.containsKey(s)){
							queryOutputs.get(s).add(outputs);
						}
						else{
							Set<Set<String>> outputsForquery=new HashSet<Set<String>>();
							outputsForquery.add(outputs);
							queryOutputs.put(s, outputsForquery);
						}
						
					 }
				 
			 }
		 }
		 NodeSelectivityEstimator nse = null;
		 for(String sql:queryOutputs.keySet()){
			 NodeHashValues hashes=new NodeHashValues();
				
				
				try {
					nse = new NodeSelectivityEstimator("/media/dimitris/T/exaremelubm100/" + "histograms.json");
				} catch (Exception e) {
					
				}
				hashes.setSelectivityEstimator(nse);
				try{
					SQLQuery query = SQLQueryParser.parse(sql, hashes);
					if(query.getInputTables().size()!=1){
						continue;
					}
					String table=query.getInputTables().get(0).getName();
					
					for(Set<String> outputs:queryOutputs.get(sql)){
						double dupl=nse.getDuplicateEstimation(table, outputs);
						if(dupl>2){
							System.out.println("table:"+table+" columns:"+outputs+" dupl. est."+dupl);
						}
					}
					
				}
				catch(Exception e){
					
				}
		 }
		 
		 System.out.println("sss");

	}

}
