package edu.uci.ics.texera.sandbox.OpenNLPexample;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import opennlp.tools.cmdline.PerformanceMonitor;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.InvalidFormatException;
import opennlp.tools.util.Span;

/**
 * 
 * This class does location NER tag extraction from abstract_100.txt.
 * 
 * See README file for more details
 *
 */
public class NameFinderExample {
   
    public static String[] Tokenize(String sentence) throws InvalidFormatException, IOException {
    	InputStream is = new FileInputStream("./src/main/java/edu/uci/ics/texera/sandbox/OpenNLPexample/en-token.bin");
     
    	TokenizerModel model = new TokenizerModel(is);
     
    	Tokenizer tokenizer = new TokenizerME(model);
    	 
    	String tokens[] = tokenizer.tokenize(sentence);
     
    	is.close();
    	
    	return tokens;
    }

    
    public static void main(String[] args) throws IOException {
    	String dataFile = "./src/main/resources/abstract_100.txt";
    	Scanner scan = new Scanner(new File(dataFile));
    	
    	InputStream is = new FileInputStream("./src/main/java/edu/uci/ics/texera/sandbox/OpenNLPexample/en-ner-location.bin");
     
    	TokenNameFinderModel model = new TokenNameFinderModel(is);
    	is.close();
     
    	NameFinderME nameFinder = new NameFinderME(model);
    	int counter = 0;
    	PerformanceMonitor perfMon = new PerformanceMonitor(System.err, "sent");
    	perfMon.start(); 
    	while(scan.hasNextLine()) { 
    		
    		String[] sentence = Tokenize(scan.nextLine());
    		Span[] spans = nameFinder.find(sentence);
    		perfMon.incrementCounter();
    		
    		//Print out the tokens of the sentence
    		if(spans.length != 0) {
    			
    			for(String s: sentence) {
    				  
    				System.out.print("["+ s + "] ");
    			}
    			System.out.println("/n");
    		}
    		
    		//Print out the offset of each 
    		for(Span s: spans) {
    			System.out.println(s.toString());
    			for(int i = s.getStart(); i < s.getEnd(); i++) {
    				 
    				System.out.println(sentence[i]);	
    				counter++;
    			}
    		}	
    		
    		if(spans.length != 0)
    			System.out.println();
    		
    	}
 
    	perfMon.stopAndPrintFinalResult();
    	System.out.println("Number of Results: " + counter);
    	scan.close();
    
    }
    

    
     
    
}
