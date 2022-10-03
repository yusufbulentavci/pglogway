package pglogway.logdir;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.nbug.hexprobe.server.telnet.EasyTerminal;
import pglogway.Main;
import pglogway.exceptions.FlushException;
import pglogway.exceptions.LogDirIoException;
import pglogway.exceptions.LogDirParsingException;
import pglogway.exceptions.LogDirTailException;

public class LogCsvTailer implements AutoCloseable {

	static final Logger logger = LogManager.getLogger(LogCsvTailer.class.getName());

	private File csvFile;
	private boolean keepRunning = true;
	private long _updateInterval = 3000;
	private long _filePointer;
	private LogTailListener tailer;

	protected int csvInd = -1;

	private int targetCsvInd;
	private String status;
	private boolean err = false;

	private long startRafTry;

	public LogCsvTailer(File csvFile, LogTailListener tailer) {
		this.csvFile = csvFile;
		this.tailer = tailer;
		status = "init";
	}

//	static int x, y, z;

	public void runProcessTheFileTail()
			throws LogDirTailException, LogDirCanNotAccessDirectory, LogDirIoException, FlushException {
		try {
			status = "run";

			while (keepRunning) {
				status = "wait tail";
				Main.sleep("runProcessTheFileTail", _updateInterval);
				status = "run";

				if (!err) {
					try {

						long len = csvFile.length();

						if (len < _filePointer) {
							// Log must have been jibbled or deleted.
							this.tailer.tailFileErrorModified(this);
							_filePointer = len;
						} else if (len > _filePointer) {
							// File must have had something added to it!
							try {
								rafCreate();

								String[] dd = runProcessTheFileTailReadCsvLine();
								while (dd != null) {
//									x++;
									if (csvInd >= targetCsvInd) {
//										y++;
										this.tailer.tailAppendCsv(this, dd);
										this.targetCsvInd = csvInd;
									}
									// Sona cok yaklasma
									if (len - _filePointer < 1000) {
										len = csvFile.length();
										if (len - _filePointer < 1000) {
											Main.sleep("sona yaklasma", 3000);
										}
									}
									dd = runProcessTheFileTailReadCsvLine();
//									if (x % 10000 == 0) {
//										logger.debug("x=" + x + " y=" + y);
//									}
								}

							} finally {
								closeReader();
							}
						}
					} catch (LogDirParsingException e) {
						err = true;
						logger.error("Failed reading file, ignoring file:" + csvFile.getPath() + " CsvInd:" + csvInd,
								e);
					}
				}

				this.tailer.tailBeforeSleeping(this);
			}
		} catch (IOException e) {
			tailer.tailError(this, "Fatal error reading log file, log tailing has stopped.", e);
			throw new LogDirTailException(e);
//			Main.fatal();
		}
		// dispose();
	}

	BufferedReader is = null;
	RandomAccessFile raf = null;

	private void updateReader() throws IOException {
		closeReader();
		this.raf = new RandomAccessFile(csvFile, "r");
		raf.seek(_filePointer);
		is = new BufferedReader(new InputStreamReader(Channels.newInputStream(raf.getChannel())));
	}

	protected String[] runProcessTheFileTailReadCsvLine() throws LogDirParsingException, LogDirIoException {

		boolean err = false;
		String[] ret = null;
		int i;

		try {
//				if (x % 10000 == 0) {
//					logger.debug("Before parse head"+System.currentTimeMillis());
//				}
			rafStorePos();
//				if (x % 10000 == 0) {
//					logger.debug("Before parse"+System.currentTimeMillis());
//				}
			ret = LogParser.parse(is);

//				if (x % 10000 == 0) {
//					logger.debug("After parse"+System.currentTimeMillis());
//				}

			rafStartTry();
//				if (x % 10000 == 0) {
//					logger.debug("After parse try"+System.currentTimeMillis());
//				}
			rafSuccess();

//				if (x % 10000 == 0) {
//					logger.debug("After parse success"+System.currentTimeMillis());
//				}

			if (err && ret != null) {
				logger.info("Csv error resolved: ret=" + Arrays.toString(ret));
			}

			return ret;
		} catch (IOException e) {
			try {
				logger.error("Unexpected csv exception in csv file:" + csvFile.getPath() + " at csvInd:" + csvInd
						+ " csv.len:", e);
				Main.sleep("csv exception", 500);
				err = true;
				rafFailed();
			} catch (IOException e1) {
				logger.error("readCsvLine", e1);
				throw new LogDirIoException(e1);
				// Main.fatal();
			}
			throw new LogDirIoException(e);
		}
	}

	private void rafStorePos() throws IOException {
		this._filePointer = raf.getFilePointer();
	}

	private void rafStartTry() throws IOException {
		this.startRafTry = raf.getFilePointer();
	}

	private void rafSuccess() throws IOException {
		this._filePointer = this.startRafTry;
		csvInd++;
	}

	private void rafCreate() throws IOException {
		updateReader();
	}

	private void rafFailed() throws IOException {
		this.startRafTry = -1;
		updateReader();
	}

	public void terminate() {
		this.keepRunning = false;
		status = "terminate";
	}

	public void status(EasyTerminal terminal) throws IOException {
		terminal.writeLine("CsvReader, ind:" + csvInd);
		terminal.writeLine("targetInd:" + targetCsvInd);
		terminal.writeLine("status:" + status);
	}

	public void report(StringBuilder sb) {
		sb.append(" CsvReader [ind:" + csvInd);
		sb.append(" targetInd:" + targetCsvInd);
		sb.append(" status:" + status + "]");
	}

	@Override
	public void close() {
		closeReader();
	}

	private void closeReader() {

		if (is != null) {
			try {
				is.close();
			} catch (Exception e) {
			}
		}
		if (raf != null) {
			try {
				raf.close();
			} catch (Exception e) {
			}
		}
	}
}
