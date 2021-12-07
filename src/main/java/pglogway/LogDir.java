package pglogway;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.nbug.hexprobe.server.telnet.EasyTerminal;

public class LogDir implements Runnable {
	static final Logger logger = LogManager.getLogger(LogDir.class.getName());

	File logDir;
	Set<String> commandTags = new HashSet<String>();

	String processCsvFileName = null;
//	boolean isToTail = false;

	private ConfDir confDir;

	private boolean taken;

	private int switchFileCount;

	private int sleepForTailCount;

	private LogFile logFile;

	private boolean alive = true;

	private String status;

	private Store store = new Store();

	private long historicCsvLines = 0;

	private int historicElasticPushed = 0;

	public void report() {
		StringBuilder sb = new StringBuilder("Report for directory:");
		sb.append(logDir.getAbsolutePath());
		sb.append(" status:");
		sb.append(status);
		sb.append(" alive:");
		sb.append(alive);
		sb.append(" CsvLines:");
		sb.append(historicCsvLines);
		sb.append(" ElasticPushed:");
		sb.append(this.historicElasticPushed);
		if (historicCsvLines != 0) {
			sb.append(" PushElimination(MergeOrFilter)Ratio:");
			sb.append((this.historicCsvLines - this.historicElasticPushed) / this.historicCsvLines);
		}
		if (logFile != null)
			logFile.report(sb);
		logger.info(sb.toString());
	}

	public LogDir(ConfDir dir, int switchFileCount, int sleepForTailCount) {
		this.confDir = dir;
		logDir = new File(dir.getPath());
		this.switchFileCount = switchFileCount;
		this.sleepForTailCount = sleepForTailCount;
		status = "Init";
	}

//	protected LogDir(String string, int switchFileCount2, int sleepForTailCount2) {
//		this(new ConfDir(string), switchFileCount2, sleepForTailCount2);
//	}

	@Override
	public void run() {
		logger.info("Starting:" + confDir);
		while (this.alive) {
			if (!switchFile(true, false)) {
				if (switchFileCount != -1 && switchFileCount == 0) {
					logger.info("Dont wait, leaving...." + this.switchFileCount);
					break;
				}

				try {
					status = "waiting csv file";
					Thread.sleep(2000);
				} catch (InterruptedException e) {
				}
				continue;
			}
			if (this.confDir.getElasticDir() || this.confDir.getAlarm()) {
				if (processCsvFileName != null) {
					status = "processing:" + processCsvFileName;

					logger.info("Working on:" + processCsvFileName);
					setLogFile(new LogFile(logDir, processCsvFileName, sleepForTailCount, confDir));
					this.logFile.process(new DirState() {

						@Override
						public boolean checkRollover() {
							return LogDir.this.switchFile(false, logFile.error);
						}
					});
					logger.info("Ends working on:" + processCsvFileName);
				}
			}
		}
		logger.info("Logdir terminating gracefully:" + this.confDir);
	}

	public boolean switchFile(boolean makeChange, boolean error) {
		String[] allCsvs = logDir.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (!name.startsWith("postgres") || !name.endsWith(".csv")) {
					return false;
				}
				if (logFile != null && logFile.csvFile.equals(name)) {
					return false;
				}
				return true;
//				File json = new File(dir, name + ".json");
//				return !json.exists();
			}
		});

		if (allCsvs == null) {
			logger.error("Access problem to directory:" + logDir.getAbsolutePath());
			System.exit(10);
			return false;
		}

		List<String> csvs = new ArrayList<String>();
		for (String string : allCsvs) {
			csvs.add(string);
		}
		Collections.sort(csvs);

		if (csvs.size() == 0) {
			return false;
		}

		if (processCsvFileName != null && csvs.get(0).equals(processCsvFileName)) {
			logger.debug("Json file is not created yet; continue with same file:" + processCsvFileName);
			return false;
		}
		if (makeChange) {
			processCsvFileName = csvs.get(0);
			this.taken = false;
			// Bug:
			if (!confDir.getElasticDir() && logFile == null && csvs.size() > 1) {
				File csvFile = new File(logDir, processCsvFileName);
				csvFile.renameTo(new File(logDir, processCsvFileName + "-done"));
				logger.info("Renamed:" + csvFile.getAbsolutePath());
			} else if (logFile != null)
				logFile.done();
		}
		if (switchFileCount > 0)
			switchFileCount--;
		return true;
	}

	public void terminate() {
		status = "terminating";
		this.alive = false;
		if (logFile != null)
			logFile.terminate();
	}

	public void status(EasyTerminal terminal) throws IOException {
		terminal.writeLine("LogDir:" + this.confDir);
		terminal.writeLine(status);
		if (logFile != null) {
			logFile.status(terminal);
		}
	}

	public void maintain(int hour) {
		if (logFile != null) {
			logFile.maintain(hour);
		}

		pgBugCleanEmptyLogFiles();

		if (confDir.getNoZip().in(hour)) {
			if (logger.isDebugEnabled())
				logger.debug("No zip hour for directory:" + this.logDir.getAbsolutePath());
		} else {
			Gzip gzip = new Gzip();
			gzip.compressFiles(logDir, confDir.getHourlyGzipTimeoutInMins(), confDir.getLetLogStayInMins(),
					confDir.isDontCopy());
		}

		if (confDir.isDontCopy()) {
			if (logger.isDebugEnabled()) {
				logger.debug("Dont copy for path returning:" + logDir.getAbsolutePath());
			}
			return;
		}

		if (confDir.getNoCopy().in(hour)) {
			if (logger.isDebugEnabled())
				logger.debug("No copy hour for directory:" + this.logDir.getAbsolutePath());
		} else {
			int count = store.doit(confDir.getStoreHost(), confDir.getStorePath(), confDir.getCluster(),
					ConfDir.getHostName(), confDir.getPort(), confDir.getPath(), confDir.getHourlyStoreTimeoutInMins(),
					confDir.isDontCopy());
			if (logger.isDebugEnabled()) {
				logger.debug("Stored file count:" + count + " for path:" + confDir.getPath());
			}
		}
	}

	private void pgBugCleanEmptyLogFiles() {
		long older = System.currentTimeMillis() - 60 * 60 * 1000 * 2;

		File[] empty = logDir.listFiles(new FileFilter() {

			@Override
			public boolean accept(File f) {
				if (!f.getName().startsWith("postgresql-")) {
					return false;
				}
				if (!f.getName().endsWith("00_00")) {
					return false;
				}

				return f.lastModified() < older && f.length() == 0;
			}
		});
		for (File file : empty) {
			file.delete();
			logger.info("Delete old csv pair file:" + file.getAbsolutePath());
		}

	}

	public void setLogFile(LogFile logFile) {
		if (this.logFile != null) {
			this.historicCsvLines += this.logFile.csvLine;
			this.historicElasticPushed += this.logFile.getElasticPushed();
		}
		this.logFile = logFile;
	}

}
