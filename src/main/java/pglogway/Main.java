package pglogway;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import net.nbug.hexprobe.server.telnet.EasyTerminal;

/**
 * Hello world!
 *
 */
public class Main {
	public static boolean testing = false;

	static final Logger logger = LogManager.getLogger(Main.class.getName());
	private List<LogDir> runningDirs = new ArrayList<>();
	private List<Thread> runningThreads = new ArrayList<>();

	private boolean shuttingDown = false;

	private static Main one;

	public static void main(String[] args) {
		one = new Main();
		Configurator.initialize(null, "/etc/pglogway-log4j2.properties");
		one.mainIn(args);
	}

	public static Main one() {
		return one;
	}

	public void status(EasyTerminal terminal) throws IOException {
		for (LogDir it : runningDirs) {
			it.status(terminal);
		}
	}

	protected void shutdownHook() {
		logger.info("Shutdown signal detected");
		this.shuttingDown = true;
		for (LogDir logDir : runningDirs) {
			logDir.terminate();
		}
		for (Thread t : runningThreads) {
			t.interrupt();
		}
		Telnet.terminate();
	}


	public void mainIn(String[] args) {
		one = this;
		
		try {
			Telnet.start();
		} catch (IOException e1) {
			logger.error("Failed to start telnet server", e1);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				shutdownHook();
			}

		});

//		args=new String[] {"--dir=ali", "--dir=veli"};
		try {
			logger.info("Application start:" + new Date().toString());
			List<ConfDir> dirs = new ArrayList<>();
			File iniFile = new File("/etc/pglogway.ini");
			if (iniFile.exists()) {
				try {
					Ini ini = new Ini("/etc/pglogway.ini");
					dirs.addAll(ini.getDirs());
				} catch (Exception e) {
					logger.error("Failed to obtaion configuration from file:/etc/pglogway.ini", e);
				}
			}

			logger.info("Directory destinations:" + dirs.toString());

			for (ConfDir conf : dirs) {
				LogDir logDir = new LogDir(conf, -1, -1);
				runningDirs.add(logDir);
				Thread t = new Thread(logDir);
				runningThreads.add(t);
				t.start();
			}

//			System.out.println(dirs);
			logger.info("Application start completed successfully:" + new Date().toString());
		} catch (Exception e) {
			logger.error("Application start ended with error:" + new Date().toString(), e);
			shutdownHook();
		}

		// Convert to schedular thread
		// Wake up ever hour
		while (!shuttingDown) {
			try {
				Calendar oldday = Calendar.getInstance(); // today
				Integer hour=oldday.get(Calendar.HOUR_OF_DAY);
				logger.info("Maintain task is running for hour:"+hour);
				for (LogDir ld : runningDirs) {
					ld.maintain(hour);
					ld.report();
				}
				Thread.sleep(1000 * 60 * 60);
			} catch (InterruptedException e) {
			}
		}

		logger.info("Schedular is leaving...");
	}
}
