package pglogway.exceptions;

import java.io.IOException;

public class ScpFailedException extends Exception {

	public ScpFailedException(String string, IOException e1) {
		super(string, e1);
	}

}
