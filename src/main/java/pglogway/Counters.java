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
	AtomicLong zipped = new AtomicLong();
	AtomicLong copied = new AtomicLong();
	AtomicLong failedZipped = new AtomicLong();
	AtomicLong failedCopied = new AtomicLong();

	public void status(EasyTerminal terminal) throws IOException {
		terminal.writeLine("filteredLogCount:" + this.filteredLogCount.get());
		terminal.writeLine("limitElasticPushCount:" + this.limitElasticPushCount.get());
		terminal.writeLine("limitPgPushCount:" + this.limitPgPushCount.get());
		terminal.writeLine("elasticSent:" + this.elasticSent.get());
		terminal.writeLine("pgSent:" + this.pgSent.get());
		terminal.writeLine("pgSent:" + this.zipped.get());
		terminal.writeLine("copied:" + this.copied.get());
		terminal.writeLine("failedZipped:" + this.failedZipped.get());
		terminal.writeLine("failedCopied:" + this.failedCopied.get());
		this.filteredLogCount.set(0);
		this.limitElasticPushCount.set(0);
		this.limitPgPushCount.set(0);
		this.elasticSent.set(0);
		this.pgSent.set(0);
		this.zipped.set(0);
		this.copied.set(0);
		this.failedZipped.set(0);
		this.failedCopied.set(0);
	}

}
