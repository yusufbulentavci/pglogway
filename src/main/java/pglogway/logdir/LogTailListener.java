package pglogway.logdir;

import pglogway.exceptions.FlushException;

public interface LogTailListener {

	void tailError(LogCsvTailer logCsvReader, String string, Exception e);

	void tailFileErrorModified(LogCsvTailer logCsvReader) throws FlushException;

	void tailAppendCsv(LogCsvTailer logCsvReader, String[] dd) throws FlushException;

	void tailBeforeSleeping(LogCsvTailer logCsvReader) throws LogDirCanNotAccessDirectory, FlushException;

}
