package edu.uci.ics.texera.sandbox.OpenNLPexample;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import opennlp.tools.cmdline.PerformanceMonitor;
import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.InvalidFormatException;

/**
 * This class does part of speech extraction from abstract_100.txt.
 *  See README file for more details.
 *
 */
public class POSTagexample {

	public static String[] Tokenize(String sentence) throws InvalidFormatException, IOException {
    	InputStream is = new FileInputStream("./src/main/java/edu/uci/ics/texera/sandbox/OpenNLPexample/en-token.bin");
     
    	TokenizerModel model = new TokenizerModel(is);
     
    	Tokenizer tokenizer = new TokenizerME(model);
    	
    	String tokens[] = tokenizer.tokenize(sentence);
     
    	is.close();
    	
    	return tokens;
	}
	
    public static void main(String[] args) throws IOException {
    	
    	POSModel model = new POSModelLoader()	
    		.load(new File("./src/main/java/edu/uci/ics/texera/sandbox/OpenNLPexample/en-pos-maxent.bin"));
    	PerformanceMonitor perfMon = new PerformanceMonitor(System.err, "sent");
    	POSTaggerME tagger = new POSTaggerME(model);
     
    	String dataFile = "./src/main/resources/abstract_100.txt";
    	Scanner scan = new Scanner(new File(dataFile));
    	
    	int counter = 0;
    	perfMon.start();
    	while(scan.hasNextLine()) {
    		String input = scan.nextLine();
    		String[] sentence = Tokenize(input);
    		
        	String[] tags = tagger.tag(sentence);
        	perfMon.incrementCounter();
        		
        	for (int i = 0; i < sentence.length; i++) {
        		String word = sentence[i];
        		String pos = tags[i];
        		//filter out useless results
        		if(!word.equals(pos) && !pos.equals("``") && !pos.equals("''")) {
        			counter++;
        			System.out.println("word: " + sentence[i] + " pos: " + tags[i]);
        		}
        	}
        	
    	}
    	 
    	System.out.println("Total Number of Results: " + counter);
    	perfMon.stopAndPrintFinalResult();
    	scan.close();
    }
}
