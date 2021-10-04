package pglogway;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

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
				long len = csvFile.length();

				if (len < _filePointer) {
					// Log must have been jibbled or deleted.
					this.tailer.fileErrorModified(this);
					_filePointer = len;
				} else if (len > _filePointer) {
					// File must have had something added to it!
					try (RandomAccessFile raf = new RandomAccessFile(csvFile, "r");) {
						raf.seek(_filePointer);

						InputStream is = Channels.newInputStream(raf.getChannel());
						CSVReader reader = new CSVReader(new InputStreamReader(is));

						String[] dd = readCsvLine(raf, reader);
						while (dd != null) {
							if (csvInd >= targetCsvInd) {
								this.tailer.appendCsv(this, dd);
								this.targetCsvInd = csvInd;
							}
							dd = readCsvLine(raf, reader);
						}

					}
				}
				this.tailer.beforeSleeping(this);
			}
		} catch (Exception e) {
			tailer.error(this, "Fatal error reading log file, log tailing has stopped.", e);
		}
		// dispose();
	}

	protected String[] readCsvLine(RandomAccessFile raf, CSVReader reader) {

		String[] ret = null;
		for (int i = 0; i < 1000 && keepRunning; i++) {
			try {
				ret = reader.readNext();
				csvInd++;
				_filePointer = raf.getFilePointer();
				return ret;
			} catch (CsvValidationException | IOException e) {
				try {
					raf.seek(_filePointer);
				} catch (IOException e1) {
					logger.error("readCsvLine", e1);
				}
			}
		}
		return ret;

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
		sb.append(" status:" + status+"]");
	}

}
