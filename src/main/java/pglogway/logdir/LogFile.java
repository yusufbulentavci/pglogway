package pglogway.logdir;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.nbug.hexprobe.server.telnet.EasyTerminal;
import pglogway.ConfDir;
import pglogway.LogLine;
import pglogway.LogWriter;
import pglogway.Main;
import pglogway.exceptions.CantFindNextDayException;
import pglogway.exceptions.ConfigException;
import pglogway.exceptions.FlushException;
import pglogway.exceptions.LogDirIoException;
import pglogway.exceptions.LogDirTailException;
import pglogway.exceptions.UnexpectedSituationException;
import pglogway.pg.PgConnection;

public class LogFile implements LogTailListener{
	static final Logger logger = LogManager.getLogger(LogFile.class.getName());

	private File f;
	private Map<String, PgConnection> connections = new HashMap<>();
	private LogWriter logWriter;

	int csvLine = 0;

	String csvFile;
	private String jsonFile;

	private File logDir;

	private int sleepForTailCount;

	protected boolean error = false;

	private LogCsvTailer reader;
	String status;

	private ConfDir confDir;
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS] x");

	PgConnection using = null;

	private DirState dirState;

	public LogFile(File logDir, String csvFile, int sleepForTailCount, ConfDir confDir) {
		status = "init";
		this.logDir = logDir;
		this.csvFile = csvFile;
		this.jsonFile = csvFile + ".json";
		this.sleepForTailCount = sleepForTailCount;
		this.confDir = confDir;
	}

	void runProcessTheFile(final DirState dirState) throws LogDirTailException, LogDirCanNotAccessDirectory, LogDirIoException, ConfigException, UnexpectedSituationException, UnknownHostException, CantFindNextDayException, FlushException {
		this.dirState=dirState;

		status = "processing";
		this.f = new File(logDir, csvFile);
		
		try(LogWriter logWriter = new LogWriter(logDir, jsonFile, csvFile, confDir);){
			this.logWriter = logWriter;
			try(LogCsvTailer reader = new LogCsvTailer(f, this);){
				this.reader = reader;
				reader.runProcessTheFileTail();
			}
		}finally {
			for (PgConnection c : connections.values()) {
				c.resetPbcc(false);
			}
		}

	}

	private void processNoSession(LogLine ll) throws FlushException {
		logWriter.write(ll, null);
	}

	public void done() {
		logWriter.done();
		status = "done";
	}

	public void terminate() {
		if (this.reader != null) {
			this.reader.terminate();
		}
		status = "terminate";
	}

	public void status(EasyTerminal terminal) throws IOException {
		terminal.writeLine("LogFile:" + csvFile);
//		terminal.writeLine("jsonFile:"+jsonFile);
		terminal.writeLine("status:" + status);
		if (reader != null) {
			reader.status(terminal);
		}
	}

	public void maintain(int hour) {
		this.logWriter.checkExpiredIndexes();
	}

	public void report(StringBuilder sb) {
		sb.append(" LogFile:[");
		sb.append(csvFile);
		sb.append(" status:");
		if (reader != null) {
			reader.report(sb);
		}
		sb.append("]");
	}

	public int getElasticPushed() {
		if (this.logWriter == null)
			return 0;
		return this.logWriter.getElasticPushed();
	}
	
	public int getPgPushed() {
		if (this.logWriter == null)
			return 0;
		return this.logWriter.getPgPushed();
	}
	
	@Override
	public void tailBeforeSleeping(LogCsvTailer logCsvReader) throws LogDirCanNotAccessDirectory, FlushException {
		logWriter.beforeSleeping();
		if (sleepForTailCount == 0) {
			logCsvReader.terminate();
			return;
		}

		if (dirState.checkRollover()) {
			logCsvReader.terminate();
			return;
		}
		sleepForTailCount--;
	}

	@Override
	public void tailFileErrorModified(LogCsvTailer logCsvReader) throws FlushException {
		logger.error("LogFile file modified:" + LogFile.this.csvFile);
		LogFile.this.error = true;
		LogLine ll = new LogLine(formatter, logCsvReader.csvInd, "ERROR", "LOG_CSV_PARSE", "csv file modified",
				confDir.getPortInt());
		processNoSession(ll);
	}

	@Override
	public void tailError(LogCsvTailer logCsvReader, String string, Exception e) {
		logger.error("LogFile error:" + LogFile.this.csvFile + " Msg:" + string, e);
//		LogFile.this.error = true;
//		LogLine ll = new LogLine(formatter, logCsvReader.csvInd, "ERROR", "LOG_CSV_PARSE", string,
//				confDir.getPortInt());
//		processNoSession(ll);
//		Main.fatal();
		
	}

	@Override
	public void tailAppendCsv(LogCsvTailer logCsvReader, String[] dd) throws FlushException {
		csvLine++;
//		try {
			LogLine ll = new LogLine(formatter, logCsvReader.csvInd, dd, confDir.getPortInt());
			if (ll.session_id != null) {
				ll.session_id = ll.session_id.trim();
				if (ll.session_id.length() > 0) {
					if (using == null || !using.sessionId.equals(ll.session_id)) {
						using = connections.get(ll.session_id);
					}
					if (using == null) {
						using = new PgConnection(logWriter, ll);
						connections.put(ll.session_id, using);
					}
					using.process(ll);
					return;
				}
			}
			processNoSession(ll);
//		} catch (Exception e) {
//			logger.error("LogFile error:" + LogFile.this.csvFile + " Failed to append for csvInd:"
//					+ logCsvReader.csvInd + " ignoring...", e);
//		}
	}

}
