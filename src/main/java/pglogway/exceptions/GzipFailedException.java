package pglogway.exceptions;

import java.io.IOException;

public class GzipFailedException extends Exception {

	public GzipFailedException(String string, IOException e) {
		super(string, e);
	}

}
