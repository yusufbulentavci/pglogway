package pglogway;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import net.nbug.hexprobe.server.telnet.EasyTerminal;

public class LogCsvReader {

	static final Logger logger = LogManager.getLogger(LogCsvReader.class.getName());

	private File csvFile;
	private boolean keepRunning = true;
	private long _updateInterval = 1000;
	private long _filePointer;
	private LogTailListener tailer;

	protected int csvInd = -1;

	private int targetCsvInd;
	private String status;
	private boolean err = false;

	private long startRafTry;

	public LogCsvReader(File csvFile, LogTailListener tailer) {
		this.csvFile = csvFile;
		this.tailer = tailer;
		status = "init";
	}

	public void run() {
		try {
			status = "run";

			while (keepRunning) {
				try {
					status = "wait tail";
					Thread.sleep(_updateInterval);
				} catch (Exception e) {
					continue;
				}
				status = "run";

				if (!err) {
					try {

						long len = csvFile.length();

						if (len < _filePointer) {
							// Log must have been jibbled or deleted.
							this.tailer.fileErrorModified(this);
							_filePointer = len;
						} else if (len > _filePointer) {
							// File must have had something added to it!
							try  {
								rafCreate();

								String[] dd = readCsvLine();
								while (dd != null) {
									if (csvInd >= targetCsvInd) {
										this.tailer.appendCsv(this, dd);
										this.targetCsvInd = csvInd;
									}
									// Sona cok yaklasma
									if (len - _filePointer < 1000) {
										len = csvFile.length();
										if (len - _filePointer < 1000) {
											Thread.sleep(3000);
										}
									}
									dd = readCsvLine();

								}

							}finally {
								closeReader();
							}
						}
					} catch (Exception e) {
						err = true;
						logger.error("Failed reading file, ignoring file:" + csvFile.getPath() + " CsvInd:" + csvInd,
								e);
					}
				}

				this.tailer.beforeSleeping(this);
			}
		} catch (Exception e) {
			tailer.error(this, "Fatal error reading log file, log tailing has stopped.", e);
			Main.fatal();
		}
		// dispose();
	}

	InputStreamReader is = null;
	RandomAccessFile raf = null;

	private void updateReader() throws IOException {
		closeReader();
		this.raf = new RandomAccessFile(csvFile, "r");
		raf.seek(_filePointer);
		is = new InputStreamReader(Channels.newInputStream(raf.getChannel()));
	}

	private void closeReader() {
		
		if (is != null) {
			try {
				is.close();
			} catch (Exception e) {
			}
		}
		if(raf!=null) {
			try {
				raf.close();
			} catch (Exception e) {
			}
		}
	}

	protected String[] readCsvLine() {

		boolean err = false;
		String[] ret = null;
		int i;
		for (i = 0; i < 1000 && keepRunning; i++) {
			try {
				rafStorePos();
				ret = LogParser.parse(is);

				rafStartTry();
//				if (ret != null && ret.length != 23) {
//					logger.error("Unexpected csv in csv file:" + csvFile.getPath() + " at csvInd:" + csvInd
//							+ " csv.len:" + ret.length + "csv:" + Arrays.toString(ret));
//					try {
//						Thread.sleep(500);
//					} catch (InterruptedException e) {
//					}
//					rafFailed();
//					err = true;
//					continue;
//				}
				rafSuccess();

				if (err && ret != null) {
					logger.info("Csv error resolved: ret=" + Arrays.toString(ret));
				}
//				System.err.println(ret.length);
				return ret;
			} catch (IOException e) {
				try {
					logger.error("Unexpected csv exception in csv file:" + csvFile.getPath() + " at csvInd:" + csvInd
							+ " csv.len:", e);
					try {
						Thread.sleep(500);
					} catch (InterruptedException eR) {
					}
					err = true;
					rafFailed();
				} catch (IOException e1) {
					logger.error("readCsvLine", e1);
					// Main.fatal();
				}
			}
		}
		if (i > 999) {
			throw new RuntimeException("Failed many times");
		}

		return ret;

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

}
