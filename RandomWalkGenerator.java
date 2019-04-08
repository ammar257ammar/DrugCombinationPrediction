
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomWalkGenerator {
	
	public static final Logger log = LoggerFactory.getLogger(RandomWalkGenerator.class);

	public static String directPropsQuery = "SELECT ?p ?o WHERE {$ENTITY$ ?p ?o}";

	/**
	 * defines the depth of the walk (only nodes are considered as a step)
	 */
	public static int depthWalk = 5;
	/**
	 * defines the number of walks from each node
	 */
	public static int numberWalks = 200;

	/**
	 * the query for extracting paths
	 */
	public static String walkQuery = "";

	public static int processedEntities = 0;
	public static int processedWalks = 0;
	public static int fileProcessedLines = 0;

	public static long startTime = 0;

	/**
	 * the rdf model
	 */
	public static Model model;

	public static Dataset dataset;

	public static String fileName = "randomwalks.txt";

	/**
	 * file writer for all the paths
	 */
	public static Writer writer;

	public void generateWalks(String repoLocation, String outputFile,
			int nmWalks, int dpWalks, int nmThreads, int offset, int limit) {
		// set the parameters
		numberWalks = nmWalks;
		depthWalk = dpWalks;
		fileName = outputFile;

		// int the writer
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputFile, false), "utf-8"),
					32 * 1024);
		} catch (UnsupportedEncodingException | FileNotFoundException e1) {
			e1.printStackTrace();
		}

		// generate the query
		walkQuery = generateQuery(depthWalk, numberWalks);

		// open the dataset
		dataset = TDBFactory.createDataset(repoLocation);

		model = dataset.getDefaultModel();
		System.out.println("SELECTING all entities from repo");
		List<String> entities = selectAllEntities(offset, limit);

		System.out.println("Total number of entities to process: "
				+ entities.size());
		ThreadPoolExecutor pool = new ThreadPoolExecutor(nmThreads, nmThreads,
				0, TimeUnit.SECONDS,
				new java.util.concurrent.ArrayBlockingQueue<Runnable>(
						entities.size()));

		startTime = System.currentTimeMillis();
		for (String entity : entities) {

			EntityThread th = new EntityThread(entity);

			pool.execute(th);

		}

		pool.shutdown();
		try {
			pool.awaitTermination(10, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Selects all entities from the repo
	 * 
	 * @return
	 */
	public static List<String> selectAllEntities(int offset, int limit) {
				
		List<String> allEntities = new ArrayList<String>();

		String queryStrign = "Select distinct ?s { { SELECT ?s Where { ?s ?p ?o	}	} UNION { SELECT ?s  Where { ?o ?p ?s . FILTER(STRSTARTS(STR(?s), \"http\")) }	}	}";

		Query query = QueryFactory.create(queryStrign);

		// Execute the query and obtain results
		QueryExecution qe = QueryExecutionFactory.create(query, model);
		ResultSet results = qe.execSelect();

		while (results.hasNext()) {
			QuerySolution result = results.next();
			allEntities.add(result.get("s").toString());
			//System.out.println(result.get("s").toString());
		}
		qe.close();
		return allEntities;
	}

	/**
	 * Adds new walks to the list; If the list is filled it is written to the
	 * file
	 * 
	 * @param tmpList
	 */
	public synchronized static void writeToFile(List<String> tmpList) {
		processedEntities++;
		processedWalks += tmpList.size();
		fileProcessedLines += tmpList.size();
		for (String str : tmpList)
			try {
				writer.write(str + "\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		if (processedEntities % 100 == 0) {
			System.out
					.println("TOTAL PROCESSED ENTITIES: " + processedEntities);
			System.out.println("TOTAL NUMBER OF PATHS : " + processedWalks);
			System.out.println("TOTAL TIME:"
					+ ((System.currentTimeMillis() - startTime) / 1000));
		}
		// flush the file
		if (fileProcessedLines > 3000000) {
			fileProcessedLines = 0;
			try {
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			int tmpNM = (processedWalks / 3000000);
			String tmpFilename = fileName.replace(".txt", tmpNM + ".txt");
			try {
				writer = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(tmpFilename, false), "utf-8"),
						32 * 1024);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * generates the query with the given depth
	 * 
	 * @param depth
	 * @return
	 */
	public static String generateQuery(int depth, int numberWalks) {
		String selectPart = "SELECT ?p ?o1";
		String mainPart = "{ $ENTITY$ ?p ?o1  ";
		String query = "";
		for (int i = 1; i < depth; i++) {
			mainPart += ". ?o" + i + " ?p" + i + "?o" + (i + 1);
			selectPart += " ?p" + i + "?o" + (i + 1);
		}
		query = selectPart + " WHERE " + mainPart + "} LIMIT " + numberWalks;

		return query;
	}

	static class EntityThread implements Runnable {

		private String entity;

		private List<String> finalList;

		public EntityThread(String entity) {
			this.entity = entity;
			finalList = new ArrayList<String>();
		}

		@Override
		public void run() {
			processEntity();
			writeToFile(finalList);

		}

		private void processEntity() {

			// get all the walks
			List<String> tmpList = new ArrayList<String>();
			String queryStr = walkQuery.replace("$ENTITY$", "<" + entity + ">");

			executeQuery(queryStr, tmpList);

			Random rand = new Random();
			for (int i = 0; i < numberWalks; i++) {
				if (tmpList.size() < 1)
					break;
				int randomNum = rand.nextInt(tmpList.size());
				if (randomNum > tmpList.size() - 1)
					randomNum = tmpList.size() - 1;
				finalList.add(tmpList.get(randomNum));

				tmpList.remove(randomNum);
			}

			// get all the direct properties
			queryStr = directPropsQuery.replace("$ENTITY$", "<" + entity + ">");
			executeQuery(queryStr, finalList);

		}

		public void executeQuery(String queryStr, List<String> walkList) {
			Query query = QueryFactory.create(queryStr);
			dataset.begin(ReadWrite.READ);
			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet resultsTmp = qe.execSelect();
			String entityShort = entity;
			ResultSet results = ResultSetFactory.copyResults(resultsTmp);
			qe.close();
			dataset.end();
			while (results.hasNext()) {
				QuerySolution result = results.next();
				String singleWalk = entityShort + "->";
				// construct the walk from each node or property on the path
				System.out.println(results.getResultVars().size());
				for (String var : results.getResultVars()) {
					try {
						// clean it if it is a literal
						if (result.get(var) != null
								&& result.get(var).isLiteral()) {
							String val = result.getLiteral(var).toString();
							val = val.replace("\n", " ").replace("\t", " ")
									.replace("->", "");
							singleWalk += val + "->";
						} else if (result.get(var) != null) {
							singleWalk += result
									.get(var)
									.toString()
									.replace("->", "")
									+ "->";
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				walkList.add(singleWalk);
			}

		}
	}

	public static void main(String[] args) throws NumberFormatException, IOException {
		System.out
				.println("USAGE:  repoLocation outputFile nmWalks dpWalks nmThreads");
		RandomWalkGenerator  generator = new RandomWalkGenerator ();
		generator.generateWalks("here","randomwalks.txt", 5, 200, 2, 0, 0);

	}
}
