Step 1 - Models
You need a trained model.

Some Lingpipe models are available at 
http://alias-i.com/lingpipe/web/models.html

Step 2 - Read model to create a chunker
File modelFile = new File(your_model);
Chunker chunker = (Chunker) AbstractExternalizable.readObject(modelFile);

Step 3 - Start chunking 
Chunking chunking = chunker.chunk(your_query);

Step 4 - Check results
Chunk set includes chunks extracted from the query. 
Each chunk result includes the start offset, end offset and the chunk type . 

for (Chunk chunk : chunking.chunkSet()) {
    int start = chunk.start();
    int end = chunk.end();
    String type = chunk.type();
}
  

For more details please visit:
http://alias-i.com/lingpipe/demos/tutorial/ne/read-me.html




