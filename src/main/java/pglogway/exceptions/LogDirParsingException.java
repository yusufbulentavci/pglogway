package pglogway.exceptions;

import java.io.IOException;

public class LogDirParsingException extends Exception{

	public LogDirParsingException(String string) {
		super(string);
	}

	public LogDirParsingException(String string, IOException e) {
		super(string, e);
	}

}
