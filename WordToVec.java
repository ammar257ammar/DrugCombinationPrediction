import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.thrift.TException;

import com.medallia.word2vec.Searcher.UnknownWordException;
import com.medallia.word2vec.Word2VecModel;
import com.medallia.word2vec.Word2VecTrainerBuilder.TrainingProgressListener;
import com.medallia.word2vec.neuralnetwork.NeuralNetworkType;
import com.medallia.word2vec.thrift.Word2VecModelThrift;
import com.medallia.word2vec.util.AutoLog;
import com.medallia.word2vec.util.Common;
import com.medallia.word2vec.util.Format;
import com.medallia.word2vec.util.ProfilingTimer;
import com.medallia.word2vec.util.ThriftUtils;


public class WordToVec {

	// This method to read the random walks file line by line
	public static List<String> readWalks(File f) throws FileNotFoundException, IOException{
		
		List<String> lines = new ArrayList<String>();
		
		try (BufferedReader br = new BufferedReader(new FileReader(f))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       lines.add(line);
		    }
		}
		
		return lines;
		
	}
	
	// create a corpus of terms from the random walks
	public static List<List<String>> partitionWalks(List<String> read){
		
		List<List<String>> words = new ArrayList<List<String>>();
		
		List<String> lineWords = new ArrayList<String>();
		
		String[] tempWords = {};
		
		for(String line: read){
			
			lineWords = new ArrayList<String>();
			
			tempWords = line.split("->");
			
			for(String word : tempWords){
				lineWords.add(word);
			}
			
			words.add(lineWords);
		}
		
		return words;
		
	}
	
	// Training Word2Vec model with CBOW neural network to generate the embeddings
	@SuppressWarnings("deprecation")
	public static void trainNetwork() throws FileNotFoundException, IOException, InterruptedException, TException{
		
		final Log LOG = AutoLog.getLog();
		
		File f = new File("randomwalks.txt");
		if (!f.exists())
	       	       throw new IllegalStateException("Please provide valid random walks file");
		
		List<String> read = readWalks(f);
		List<List<String>> partitioned = partitionWalks (read);
		
		Word2VecModel model = Word2VecModel.trainer()
				.setMinVocabFrequency(5)
				.useNumThreads(20)
				.setWindowSize(8)
				.type(NeuralNetworkType.CBOW)
				.setLayerSize(200)
				.useNegativeSamples(25)
				.setDownSamplingRate(1e-4)
				.setNumIterations(5)
				.setListener(new TrainingProgressListener() {
					public void update(Stage stage, double progress) {
						System.out.println(String.format("%s is %.2f%% complete", Format.formatEnum(stage), progress * 100));
					}
				})
				.train(partitioned);
		
		// Writes model to a thrift file
		try (ProfilingTimer timer = ProfilingTimer.create(LOG, "Writing output to file")) {
			FileUtils.writeStringToFile(new File("dcdb.model"), ThriftUtils.serializeJson(model.toThrift()));
		}

	}
	
	// Create a file containing one line for each drug in DCDB with a vector of length 200 contains the embeddings
	
	public static void createEmbeddingsFile() throws IOException, TException{
				
		String json = Common.readFileToString(new File("dcdb.model"));
		Word2VecModel model = Word2VecModel.fromThrift(ThriftUtils.deserializeJson(new Word2VecModelThrift(), json));

		System.out.println(model.toThrift().getVocabSize()); 
		System.out.println(model.toThrift().getVectorsSize()); 
		System.out.println();
		
		Word2VecModelThrift m2 = model.toThrift();
		
		long lStartTime = System.nanoTime();
		
		List<Double> vecHandler = m2.getVectors();
		List<String> vocabHandler = m2.getVocab();
		int layerSize = m2.getLayerSize();
		
		DoubleBuffer vectors = DoubleBuffer.allocate(vecHandler.size());
		
		for (Double value : vecHandler)
		    vectors.put(value);
		
		
		try (BufferedWriter br = new BufferedWriter(new FileWriter("feature.vectors"))) {
		
			Integer index;
			double[] vD;
			String line = "";
			
			for(String vocab: vocabHandler){
				
				if(vocab.contains("/dcdb:")){
					
					index = m2.getVocab().indexOf(vocab);	
					
					System.out.println(vocab);
					vectors.position(index*layerSize);
					
					vD = new double[layerSize];
					
					vectors.get(vD);
					
					line = "";
					line += vocab + "\t";
					
					for(int i =0; i < layerSize-1 ; i++){
						line += String.valueOf(vD[i]) + "\t";
					}
					line += vD[layerSize-1];
					
					br.write(line+"\n");
					
				}			
			}
			
		}
		
		long lEndTime = System.nanoTime();
		
		long output = lEndTime - lStartTime;

	    System.out.println("Elapsed time in milliseconds: " + output / 1000000);
	    
	}
	
	
	public static void main(String[] args) throws InterruptedException, IOException, UnknownWordException, TException {
		
		trainNetwork();
		
		createEmbeddingsFile();
					
	}
	
}
