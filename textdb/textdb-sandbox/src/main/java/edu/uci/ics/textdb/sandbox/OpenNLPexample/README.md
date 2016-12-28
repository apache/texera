Please find trained Models here http://opennlp.sourceforge.net/models-1.5/

Step 1 - Tokenization 

    - Download the model en-token.bin
    - Load model:
        InputStream is = new FileInputStream(token_model);
        TokenizerModel model = new TokenizerModel(is);
    - Start tokenizing:
        Tokenizer tokenizer = new TokenizerME(model);
    - Tokens stored in array
        String tokens[] = tokenizer.tokenize(your_sentence);
	
Step 2 - Tagging

	POSTagging
        - Download a POS model from the link above.
        - Load model:
            POSModel model = new POSModelLoader().load(new File(pos_model));
        - Create tagger:
            POSTaggerME tagger = new POSTaggerME(model);
        - Start tagging:
            String[] tags = tagger.tag(tokens from step 1);
            Each tag is corresponding to each token in the token list
			
	NameFinder
		- Download a NER model from the link above.
		- Load model:
            InputStream is = new FileInputStream(ner_model);
            TokenNameFinderModel model = new TokenNameFinderModel(is);
    	- Create a Name Finder:
            NameFinderME nameFinder = new NameFinderME(model);
        - A span includes offsets of all ner tags.
    		Span[] spans = nameFinder.find(tokens from step 1);
		