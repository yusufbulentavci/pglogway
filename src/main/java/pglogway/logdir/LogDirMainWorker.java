package pglogway.logdir;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.nbug.hexprobe.server.telnet.EasyTerminal;
import pglogway.ConfDir;
import pglogway.Gzip;
import pglogway.Main;
import pglogway.Store;
import pglogway.exceptions.CantFindNextDayException;
import pglogway.exceptions.ConfigException;
import pglogway.exceptions.FlushException;
import pglogway.exceptions.GzipFailedException;
import pglogway.exceptions.LogDirIoException;
import pglogway.exceptions.LogDirTailException;
import pglogway.exceptions.ScpFailedException;
import pglogway.exceptions.UnexpectedSituationException;

public class LogDirMainWorker implements Runnable, DirState {
	static final Logger logger = LogManager.getLogger(LogDirMainWorker.class.getName());

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

	private int historicPgPushed;

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
		sb.append(" PgPushed:");
		sb.append(this.historicPgPushed);
		if (historicCsvLines != 0) {
			sb.append(" PushElimination(MergeOrFilter)Ratio:");
			sb.append((this.historicCsvLines - this.historicElasticPushed) / this.historicCsvLines);
		}
		if (logFile != null)
			logFile.report(sb);
		logger.info(sb.toString());
	}

	public LogDirMainWorker(ConfDir dir, int switchFileCount, int sleepForTailCount) {
		this.confDir = dir;
		logDir = new File(dir.getPath());
		this.switchFileCount = switchFileCount;
		this.sleepForTailCount = sleepForTailCount;
		status = "Init";

		if (logger.isDebugEnabled()) {
			logger.debug("configured directory:");
			logger.debug(this.toString());
		}
	}

	@Override
	public void run() {
		logger.info("Starting:" + confDir);
		try {
			while (this.alive) {
				if (!runSwitchFile(true, false)) {
					if (switchFileCount != -1 && switchFileCount == 0) {
						logger.info("Dont wait, leaving...." + this.switchFileCount);
						break;
					}

					status = "waiting csv file";
					Main.sleep("Mainloop waiting csv file", 2000);
					continue;
				}
				if (this.confDir.isPushPg() || this.confDir.getElasticDir() || this.confDir.getAlarm()) {
					if (processCsvFileName != null) {
						status = "processing:" + processCsvFileName;
						logger.info("Working on:" + processCsvFileName);
						setLogFile(new LogFile(logDir, processCsvFileName, sleepForTailCount, confDir));
						this.logFile.runProcessTheFile(this);
						logger.info("Ends working on:" + processCsvFileName);
					}
				}
			}
			logger.info("Logdir terminating gracefully:" + this.confDir);
		} catch (LogDirTailException | LogDirCanNotAccessDirectory | LogDirIoException | UnknownHostException
				| ConfigException | UnexpectedSituationException | CantFindNextDayException | FlushException e) {
			logger.error("Exception in :" + confDir + " Exiting", e);
			Main.sonlan();
//			Main.fatal();
		}
	}

	@Override
	public boolean checkRollover() throws LogDirCanNotAccessDirectory {
		return runSwitchFile(false, logFile.error);
	}

	public boolean runSwitchFile(boolean makeChange, boolean error) throws LogDirCanNotAccessDirectory {
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
			}
		});

		if (allCsvs == null) {
			logger.error("Access problem to directory:" + logDir.getAbsolutePath());
			throw new LogDirCanNotAccessDirectory("Access problem to directory:" + logDir.getAbsolutePath());
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
		if (confDir.isDontCopy()) {
			terminal.writeLine("Store method is remove or empty. Do not store files");
		} else {
			terminal.writeLine("Store method:" + confDir.getStoreMethod());
		}

		terminal.writeLine("NoCopy hours:" + confDir.getNoCopy().toString());
		terminal.writeLine("NoZip hours:" + confDir.getNoZip().toString());

		if (logFile != null) {
			logFile.status(terminal);
		}
	}

	public void maintain(int hour) throws GzipFailedException, ScpFailedException, UnknownHostException {
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
		if (empty == null) {
			return;
		}
		for (File file : empty) {
			file.delete();
			logger.info("Delete old csv pair file:" + file.getAbsolutePath());
		}

	}

	public void setLogFile(LogFile logFile) {
		if (this.logFile != null) {
			this.historicCsvLines += this.logFile.csvLine;
			this.historicElasticPushed += this.logFile.getElasticPushed();
			this.historicPgPushed += this.logFile.getPgPushed();
		}
		this.logFile = logFile;
	}

}
