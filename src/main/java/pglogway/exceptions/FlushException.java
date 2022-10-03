package pglogway.exceptions;

import java.sql.SQLException;

public class FlushException extends Exception {

	public FlushException(SQLException e) {
		super(e);
	}
	
}
