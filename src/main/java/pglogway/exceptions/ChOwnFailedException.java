package pglogway.exceptions;

import java.io.IOException;

public class ChOwnFailedException extends Exception {

	public ChOwnFailedException(String string, IOException e) {
		super(string, e);
	}

}
