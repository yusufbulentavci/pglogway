package pglogway;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pglogway.elastic.ElasticPush;
import pglogway.exceptions.CantFindNextDayException;
import pglogway.exceptions.ConfigException;
import pglogway.exceptions.FlushException;
import pglogway.exceptions.UnexpectedSituationException;
import pglogway.pg.PgPush;

public class LogWriter implements AutoCloseable{
	static final Logger logger = LogManager.getLogger(LogWriter.class.getName());

	final File file;

	private FileWriter fileWriter;

	private File fileTmp;

	private final String jsonFileName;

	private final ElasticPush elasticPush;
	private final PgPush pgPush;
	
	private final Alarm alarm;

	private final File csvFile;

	private final File csvFileDone;

	private final String csvFileName;

	private final int hour;

	private final ConfDir confDir;

	public LogWriter(File dir, String jsonFileName, String csvFileName, ConfDir confDir) throws ConfigException, UnexpectedSituationException, UnknownHostException, CantFindNextDayException {
		this.jsonFileName = jsonFileName;
		this.file = new File(dir, jsonFileName);
		this.csvFileName = csvFileName;
		this.csvFile = new File(dir, csvFileName);
		this.confDir = confDir;
		
		if(logger.isDebugEnabled()) {
			logger.debug("Logwriter is initializing for:"+dir.getPath());
		}

		if (!csvFileName.startsWith("postgresql-")) {
			throw new RuntimeException("Unexpected file name, should be start with postgres-:" + csvFileName);
		}
		String pattern = "postgresql-(\\d+)-(\\d+)-(\\d+)_(\\d+)(.*)";

		// Create a Pattern object
		Pattern r = Pattern.compile(pattern);

		// Now create matcher object.
		Matcher m = r.matcher(csvFileName);

		if (m.find()) {
			String year = m.group(1);
			String month = m.group(2);
			String day = m.group(3);
			
			if(logger.isDebugEnabled()) {
				logger.debug("Logwriter got a match;"+year+"-"+month+"-"+day);
			}

			this.hour = Integer.parseInt(m.group(4));

			if (confDir.getElasticDir()) {
				this.elasticPush = new ElasticPush(confDir, year, month, day, hour);
			} else {
				this.elasticPush = null;
			}
			
			if(confDir.isPushPg()) {
				this.pgPush = new PgPush(confDir, year, month, day, hour, confDir.getExpireDays());
			}else {
				this.pgPush = null;
			}

			if (confDir.getAlarm()) {
				this.alarm = new Alarm(confDir.getAlarmLevel());
			} else {
				this.alarm = null;
			}

			if (!Main.testing) {
				if (this.elasticPush != null)
					this.elasticPush.connect();
				if (this.pgPush != null)
					this.pgPush.connect();
			}
		} else {
			throw new UnexpectedSituationException("Unexpected file name, should be start with postgres-:" + csvFileName);
		}

		String date = csvFileName.substring(11, 21);

		this.csvFileDone = new File(dir, csvFileName + "-done");
//
//		if (this.file.exists()) {
//			Integer resumeInd = resumeIndex();
//			if (resumeInd == null || resumeInd == 0) {
//				this.file.delete();
//			}
//		}

		if (Main.testing) {
			try {
				this.fileWriter = new FileWriter(file, true);
			} catch (IOException e) {
				logger.error("Unexpected log writer error:" + file.getPath(), e);
				throw new RuntimeException(e);
			}
		}

	}

//	private Integer resumeIndex() {
//		String prevLine = null;
//		try (BufferedReader br = new BufferedReader(new FileReader(file));) {
//
//			String lastLine = br.readLine();
//			while (lastLine != null) {
//				prevLine = lastLine.length() == 0 ? prevLine : lastLine;
//				lastLine = br.readLine();
//			}
//			if (prevLine == null) {
//				return 0;
//			}
//
//			JSONObject jo = new JSONObject(prevLine);
//
//			return jo.optInt("csv-ind");
//		} catch (IOException e) {
//			if (prevLine == null)
//				logger.error("Failed resume index:", e);
//			else
//				logger.error("Failed resume index:"+prevLine, e);
//			return null;
//		}
//	}

	public void write(LogLine ll, AtomicInteger sentLogCount) throws FlushException {
//		logger.info(ll.toJson(fn).toString());
		if (this.alarm != null) {
			this.alarm.check(ll);
		}
		
		if (ll.canBeFiltered() && filtered(ll)) {
			Counters.one().filteredLogCount.incrementAndGet();
			return;
		}
		ll.setCsv(jsonFileName);
		if (!Main.testing) {
			if (this.elasticPush != null) {
				if (sentLogCount != null) {
					int val=sentLogCount.get();
					if(val > confDir.getEcon().getSentLimit()) {
						Counters.one().limitElasticPushCount.incrementAndGet();
						return;
					}
					sentLogCount.incrementAndGet();
				}
				this.elasticPush.push(ll);
				Counters.one().elasticSent.incrementAndGet();
			}
			if (this.pgPush != null) {
				if (sentLogCount != null) {
					int val=sentLogCount.get();
					if(val > confDir.getPpCon().getSentLimit()) {
						Counters.one().limitPgPushCount.incrementAndGet();
						return;
					}
					sentLogCount.incrementAndGet();
				}
				this.pgPush.push(ll);
				Counters.one().pgSent.incrementAndGet();
			}
		}

		if (Main.testing) {
			try {
				this.fileWriter.write(ll.toJson(jsonFileName).toString());
				this.fileWriter.write(System.lineSeparator());
				this.fileWriter.flush();

//				if (logger.isDebugEnabled()) {
//					logger.debug(ll.toString());
//					logger.debug(ll.toJson(jsonFileName));
//				}
			} catch (IOException e) {
				logger.error("Error in line:" + ll.toString(), e);
			}

			if (logger.isDebugEnabled()) {
				logger.debug(ll.toString());
//				logger.debug(ll.toJson(jsonFileName));
			}
		}

	}

	private boolean filtered(LogLine ll) {
		if (confDir.getFilterCommand() != null && ll.command_tag != null) {
			if (confDir.getFilterCommand().filter(ll.command_tag))
				return true;
		}
		if (confDir.getFilterDb() != null && ll.database_name != null) {
			if (confDir.getFilterDb().filter(ll.database_name))
				return true;
		}
		if (confDir.getFilterUser() != null && ll.user_name != null) {
			if (confDir.getFilterUser().filter(ll.user_name))
				return true;
		}
		if (confDir.getFilterLevel() != null && ll.error_severity != null) {
			if (confDir.getFilterCommand().filter(ll.error_severity))
				return true;
		}
		if (confDir.getFilterMinDuration() != null && ll.getDuration() != null) {
			if (confDir.getFilterMinDuration() > ll.getDuration().doubleValue())
				return true;
		}
		return false;
	}

	
	public void done() {
		this.csvFile.renameTo(csvFileDone);
		logger.info("LogFile done/renamed:" + csvFile);
	}

	public void beforeSleeping() throws FlushException {
		if (!Main.testing) {
			if (this.elasticPush != null)
				this.elasticPush.flush();
			if (this.pgPush != null)
				this.pgPush.flush();
		}

	}

	public void checkExpiredIndexes() {
		if (this.elasticPush != null)
			this.elasticPush.checkExpiredIndexes();
		if (this.pgPush != null)
			this.pgPush.checkExpiredIndexes();
	}

	public int getElasticPushed() {
		if (this.elasticPush == null)
			return 0;
		return this.elasticPush.getPushed();
	}

	public int getPgPushed() {
		if (this.pgPush == null)
			return 0;
		return this.pgPush.getPushed();
	}

	public void close() {
		logger.info("LogFile close:" + csvFile);

		if (!Main.testing) {
			if (this.elasticPush != null)
				this.elasticPush.close();
			if (this.pgPush != null)
				this.pgPush.close();
		}
		if (Main.testing) {
			try {
				this.fileWriter.close();
			} catch (IOException e) {
				logger.error("Failed logfile done; file:" + csvFile.getPath(), e);
			}
		}

	}

}
