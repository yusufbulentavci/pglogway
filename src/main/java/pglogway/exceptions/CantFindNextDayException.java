package pglogway.exceptions;

public class CantFindNextDayException extends Exception {

	public CantFindNextDayException(String string, Exception e) {
		super(string, e);
	}

}
