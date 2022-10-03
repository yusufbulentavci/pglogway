package pglogway.exceptions;

import java.io.IOException;

public class LogDirIoException extends Exception{

	public LogDirIoException(IOException e1) {
		super(e1);
	}

}
