
import java.io.IOException;
import java.io.FileReader;
import java.util.ArrayList;


public class Parser {
	private Scanner scanner;
	private Token currentToken;
	
	private ArrayList<String> regexList;
	private ArrayList<String> dictList;

	public Parser(String AQLFile) {
		regexList = new ArrayList<String>();
		dictList = new ArrayList<String>();
		
		try {
			this.scanner = new Scanner(new FileReader(AQLFile));
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.currentToken = scanner.next();
	}
	
    private boolean have(Token.Kind kind)
    {
        return currentToken.is(kind);
    }

    private boolean accept(Token.Kind kind)
    {
        if (have(kind)) {
            currentToken = scanner.next();
            return true;
        }
        return false;
    }    
        
	
	public void parse() {
		while (!have(Token.Kind.EOF)) {
			if (accept(Token.Kind.REGEX)) {
				regexList.add(currentToken.lexeme());
			} else if (accept(Token.Kind.DICTIONARY)) {
				// create dictionary '<dictionary name>' from file '<file name>'
				if (accept(Token.Kind.IDENTIFIER) && accept(Token.Kind.FROM) && accept(Token.Kind.FILE)) {
					if (have(Token.Kind.LITERAL)) {
						dictList.add(currentToken.lexeme());
					}
				}
			}
			currentToken = scanner.next();
		}
	}
	
	public ArrayList<String> getRegexList() {
		return this.regexList;
	}
	
	public ArrayList<String> getDictList() {
		return this.dictList;
	}
	
	public void printRegexList(){
		for(int i = 0; i < regexList.size(); i++ )
			System.out.println(regexList.get(i));
	}
	
	public void printDictList(){
		for(int i = 0; i < dictList.size(); i++ )
			System.out.println(dictList.get(i));
	}
	
}
