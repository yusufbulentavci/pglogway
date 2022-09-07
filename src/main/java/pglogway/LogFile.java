package pglogway;

import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.nbug.hexprobe.server.telnet.EasyTerminal;

public class LogFile {
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

	private LogCsvReader reader;
	String status;

	private ConfDir confDir;
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS] x");

	public LogFile(File logDir, String csvFile, int sleepForTailCount, ConfDir confDir) {
		status = "init";
		this.logDir = logDir;
		this.csvFile = csvFile;
		this.jsonFile = csvFile + ".json";
		this.sleepForTailCount = sleepForTailCount;
		this.confDir = confDir;
	}

	void process(final DirState dirState) {

		status = "processing";
		this.f = new File(logDir, csvFile);
		this.logWriter = new LogWriter(logDir, jsonFile, csvFile, confDir);

		this.reader = new LogCsvReader(f, new LogTailListener() {
			PgConnection using = null;

			@Override
			public void beforeSleeping(LogCsvReader logCsvReader) {
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
			public void fileErrorModified(LogCsvReader logCsvReader) {
				logger.error("LogFile file modified:" + LogFile.this.csvFile);
				LogFile.this.error = true;
				LogLine ll = new LogLine(formatter, logCsvReader.csvInd, "ERROR", "LOG_CSV_PARSE", "csv file modified",
						confDir.getPortInt());
				processNoSession(ll);
			}

			@Override
			public void error(LogCsvReader logCsvReader, String string, Exception e) {
				logger.error("LogFile error:" + LogFile.this.csvFile + " Msg:" + string, e);
				Main.fatal();
				LogFile.this.error = true;
				LogLine ll = new LogLine(formatter, logCsvReader.csvInd, "ERROR", "LOG_CSV_PARSE", string,
						confDir.getPortInt());
				processNoSession(ll);
			}

			@Override
			public void appendCsv(LogCsvReader logCsvReader, String[] dd) {
				csvLine++;
				try {
					if(dd[0].equals("2022-09-07 11:00:28.222 GMT")) {
						System.out.println("");
					}
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
				} catch (Exception e) {
					logger.error("LogFile error:" + LogFile.this.csvFile + " Failed to append for csvInd:"
							+ logCsvReader.csvInd + " ignoring...", e);
				}
			}
		});
		reader.run();
		processEndOfFile();
	}

	private void processEndOfFile() {
		for (PgConnection c : connections.values()) {
			c.resetPbcc(false);
		}
		logWriter.close();
	}

	private void processNoSession(LogLine ll) {
		logWriter.write(ll, true, null);
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

}
