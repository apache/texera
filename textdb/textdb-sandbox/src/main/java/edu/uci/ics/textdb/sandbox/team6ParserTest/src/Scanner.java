

import java.io.IOException;
import java.io.Reader;

public class Scanner {	
	private int lineNum;  // current line count
	private int charPos;  // character offset for current line
	private int nextChar; // contains the next char (-1 == EOF)
	private Reader input;
	

	Scanner(Reader reader)
	{
		input = reader;
		lineNum = 1;
		charPos = 0;
		readChar();
	}
	
	// read next char, store it into variable nextChar, increment charPos
	private void readChar()
	{
		try {
			charPos += 1;
			nextChar = input.read();
		} catch (IOException e) {
			nextChar =  0;
		}
	}
	
		

	/* Invariants:
	 *  1. call assumes that nextChar is already holding an unread character
	 *  2. return leaves nextChar containing an untokenized character
	 */
	public Token next()
	{
		Token returnToken;				// token to return
		int currentCharPos = charPos;	// current char position
		StringBuilder lexemeBuilder = new StringBuilder();	// StringBuilder incase need to use it
		
		if (nextChar == -1) {
			// EOF
			returnToken = Token.EOF(lineNum, currentCharPos);
		} else if (nextChar == '\n') {
			// new line
			readChar();
			lineNum += 1;
			charPos = 1;
			returnToken = this.next();
		} else if (nextChar == '\r' || nextChar == '\t' || nextChar == ' ') {
			// whitespace
			readChar();
			returnToken = this.next();
		} else if (nextChar == '\'') {
			// single quote
			while (true) {
				readChar();
				if (nextChar == '\'') {
					// if it's single quote, break;
					returnToken = Token.LITERAL(lexemeBuilder.toString(), lineNum, currentCharPos);
					readChar();
					break;
				} else if (nextChar == '\\') {
					// escape the next char
					lexemeBuilder.append((char) nextChar);
					readChar();
					lexemeBuilder.append((char) nextChar);
				} else {
					// else, append
					lexemeBuilder.append((char) nextChar);
				}
			}
		} else if (nextChar == '/') {
			// forward slash
			while (true) {
				readChar();
				if (nextChar == '/') {
					returnToken = Token.LITERAL(lexemeBuilder.toString(), lineNum, currentCharPos);
					readChar();
					break;
				} else if (nextChar == '\\') {
					// escape the next char
					lexemeBuilder.append((char) nextChar);
					readChar();
					lexemeBuilder.append((char) nextChar);
				} else {
					// else, append
					lexemeBuilder.append((char) nextChar);
				}
			}
		} else if (nextChar == '\"') {
			// forward slash
			while (true) {
				readChar();
				if (nextChar == '\"') {
					returnToken = Token.IDENTIFIER(lexemeBuilder.toString(), lineNum, currentCharPos);
					readChar();
					break;
				} else if (nextChar == '\\') {
					// escape the next char
					lexemeBuilder.append((char) nextChar);
					readChar();
					lexemeBuilder.append((char) nextChar);
				} else {
					// else, append
					lexemeBuilder.append((char) nextChar);
				}
			}
		}
		else if (Character.isLetter(nextChar) || (nextChar == '_')) {
			// starts with letter or underscore _
			lexemeBuilder.append((char) nextChar);
			while (true) {
				readChar();
				if (Character.isLetterOrDigit(nextChar) || (nextChar == '_')) {
					// if nextChar is letter/_/digit
					lexemeBuilder.append((char) nextChar);
				} else {
					returnToken = new Token(lexemeBuilder.toString(), lineNum, currentCharPos);
					break;
				}
			}
		} else {
			returnToken = new Token(Character.toString((char) nextChar), lineNum, currentCharPos);
			readChar();
		}

		return returnToken;
	}


	// OPTIONAL: any other methods that you find convenient for implementation or testing
}
