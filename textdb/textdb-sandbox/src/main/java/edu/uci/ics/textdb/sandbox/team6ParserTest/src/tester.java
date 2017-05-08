
public class tester {
	public static void main(String[] args){
		String aqlModuleName = "dateTime.aql";
		String aqlModulePath = ""; //"/Volumes/MacDATA/document/GoogleDrive/Spring/CS290/JavaOldWorkspace/ParserTest/";
		
			Parser parser = new Parser(aqlModulePath+aqlModuleName);
			parser.parse();
			System.out.println("Dict List: ");
			parser.printDictList();
			System.out.println("Regex List: ");
			parser.printRegexList();
	}
}
