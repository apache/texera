NOTE: Don't remove the file named .gitignore in any directory! 
		Read the comments in the files mentioned here!
		
Step 1-Prepare Data Set:

By default, there is a small data set named "abstract_100.txt" in ./sample-data-files/.
If you want to get results from more data sets, please put the data files into ./sample-data-files/,
or specify another path to the data sets when running WriteIndex.java in the next step. 

More data sets can be found at:
https://github.com/Texera/texera/wiki/Data-Sets
 
Step 2-Prepare Index:
In order to write index, run WriteIndex.java file in edu.uci.ics.texera.perftest.run package.

By default, indices are stored in ./index/standard/ and ./index/trigram/.
If you want to store the indices to somewhere else, you can specify the 
standard index path or trigram index path when running WriteIndex.

NOTE: Once index is created, it can be reused (no writing index needed in the future)!!!!
		However, if you make changes to your data set(s), please clean up the old 
		indices and writing index is still needed.

Step 3-Prepare Queries:
By default, there is a sample query file named "sample_queries.txt" in ./perftest-files/queries/.
These queries are used in all operator performance tests except regexmatcher.
The default regex queries are defined in RunPerftests.java.

If you are satisfied with the default queries, then move onto Step 4.

If you want to use your own queries, please put query files to ./perftest-files/queries/ or
specify the query file's path when running RunPerftests in the next step.

If you want to use your own regex queries, please change the regex queries listed in RunPerftests.java.

In RunPerftests.java, you can see some operator performance tests take in a query file as a parameter.
Please change the query file name to the one you want.
 
Step 4-Run Performance Tests:
In order to run the performance tests, please run RunPerftests.java file in edu.uci.ics.texera.perftest.run package.

If indices are stored at somewhere other than ./index/standard/ and ./index/trigram/, please
specify the standard index path and trigram index path before running the class.

If you have a specific location for the test results to be stored, please specify it before running the class.

Step 5-Review Test Results:
Performance test results can be found in the corresponding csv files in ./perftest-files/results/
or the result folder you previously specify.
For example, test results for regex matcher can be found in ./perftest-files/results/regex.csv.
 