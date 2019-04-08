import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.FileManager;

// Before generating the random walks we have to import RDF triple data into local RDF triple store repository
// because random wals are generated from a sparql query on the rdf store
// This file is run only once then we can use the other files without looking back here

public class BulkLoader {

	public static void loadDCDB(String storageDir, String inputDir) {

		System.out.println("WE start the code!!");
		Dataset dataset = TDBFactory.createDataset(storageDir);

		// assume we want the default model, or we could get a named model here
		
		Model tdb = dataset.getDefaultModel();

		// read the input file - only needs to be done once

		try {
			FileManager.get().readModel(tdb, inputDir, "N-TRIPLES");
		} catch (Exception e) {
			System.out.println("File didn't finish");
			e.printStackTrace();
		}
	
		try {
			// run a query to make sure every thing imported successfully
			String q = "select (count(distinct ?s) as ?c) where {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?t} ";
			Query query = QueryFactory.create(q);
			QueryExecution qexec = QueryExecutionFactory.create(query, tdb);
			ResultSet results = qexec.execSelect();
			ResultSetFormatter.out(System.out, results, query);
		} catch (Exception e) {
			System.out.println("The stupid query failed");
		}
		dataset.close();
	}

	public static void main(String[] args) {

		// Import the triples file into local RDF triple store repository names "here"
		loadDCDB("here", "output.nq");
		
	}

}
