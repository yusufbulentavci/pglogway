package pglogway;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Alarm {
	static final Logger logger = LogManager.getLogger(Alarm.class);
// INFO, NOTICE, WARNING, ERROR, LOG, FATAL, and PANIC.
	private String alarmLevelStr;
	private int alarmLevel;
	private File file = new File("/tmp/pglogway.alarm");

	public Alarm(String alarmLevel) {
		this.alarmLevelStr = alarmLevel;
		this.alarmLevel = severity(alarmLevel);
		logger.info("Alarm is on; and level is "+alarmLevelStr);
		if(file.exists()) {
			file.delete();
		}
	}

	public void check(LogLine ll) {
		if (alarmLevel == 0 || ll.error_severity == null) {
			return;
		}

		int sev = severity(ll.error_severity);
		if (sev >= alarmLevel) {
			fire();
		}
	}

	private void fire() {
		touch();
	}

	public void touch() {
		if (!file.exists()) {
			try {
				new FileOutputStream(file).close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else {
			long timestamp = System.currentTimeMillis();
			file.setLastModified(timestamp);
		}

	}

	private int severity(String error_severity) {

		switch (error_severity) {
		case "INFO":
			return 4;
		case "NOTICE":
			return 5;
		case "LOG":
			return 6;
		case "WARNING":
			return 7;
		case "ERROR":
			return 8;
		case "FATAL":
			return 9;
		case "PANIC":
			return 10;
		}
		return 0;
	}

}
