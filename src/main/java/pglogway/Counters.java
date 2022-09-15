package pglogway;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import net.nbug.hexprobe.server.telnet.EasyTerminal;

public class Counters {
	private static Counters one = new Counters();

	static Counters one() {
		return one;
	}

	AtomicLong filteredLogCount = new AtomicLong();
	AtomicLong limitElasticPushCount = new AtomicLong();
	AtomicLong limitPgPushCount = new AtomicLong();
	AtomicLong elasticSent = new AtomicLong();
	AtomicLong pgSent = new AtomicLong();

	public void status(EasyTerminal terminal) throws IOException {
		terminal.writeLine("filteredLogCount:" + this.filteredLogCount.get());
		terminal.writeLine("limitElasticPushCount:" + this.limitElasticPushCount.get());
		terminal.writeLine("limitPgPushCount:" + this.limitPgPushCount.get());
		terminal.writeLine("elasticSent:" + this.elasticSent.get());
		this.filteredLogCount.set(0);
		this.limitElasticPushCount.set(0);
		this.limitPgPushCount.set(0);
		this.elasticSent.set(0);
		this.pgSent.set(0);
	}

}
