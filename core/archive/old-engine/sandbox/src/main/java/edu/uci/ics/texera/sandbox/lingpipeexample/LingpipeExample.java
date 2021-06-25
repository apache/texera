package edu.uci.ics.texera.sandbox.lingpipeexample;

import com.aliasi.chunk.Chunker;
import com.aliasi.chunk.Chunking;

import com.aliasi.util.AbstractExternalizable;

import java.io.File;
import java.util.Scanner;

import com.aliasi.chunk.Chunk;
 

public class LingpipeExample {

    public static void main(String[] args) throws Exception {
  
        String model = "./src/main/java/edu/uci/ics/texera/sandbox/lingpipeexample/ne-en-bio-genetag.HmmChunker";
        String dataFile = "./src/main/resources/abstract_100.txt";
        File modelFile = new File(model);
        System.out.println("Reading chunker from file=" + modelFile);
        Chunker chunker 
            = (Chunker) AbstractExternalizable.readObject(modelFile);
        
        Scanner scan = new Scanner(new File(dataFile));

        
        int results = 0;
        long startTime = System.currentTimeMillis();
        
        while(scan.hasNextLine()){ 
            String line = scan.nextLine();
            if(line.isEmpty()) continue;
            Chunking chunking = chunker.chunk(line);
            for (Chunk chunk : chunking.chunkSet()) {
                int start = chunk.start();
                int end = chunk.end();
                String type = chunk.type();
                String phrase = line.substring(start,end);
                System.out.println("     text= " + phrase 
                                   + " type=" + type);
                results++;
            }
            System.out.println("\n");
        }
        
        if(scan != null) {
            scan.close();
        }
        
        long endTime = System.currentTimeMillis();
        
        double timeSeconds = (endTime - startTime) / 1000.0;
        System.out.println("total time: " + timeSeconds);
        System.out.println("total results: " + results);
     }

}