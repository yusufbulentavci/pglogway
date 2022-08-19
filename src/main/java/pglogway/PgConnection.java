package pglogway;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PgConnection {
	static final Logger logger = LogManager.getLogger(PgConnection.class.getName());

	public PgConnection(LogWriter logWriter, LogLine ll) {
		this.logWriter = logWriter;
		sessionId = ll.session_id;
	}

	List<LogLine> unknown = new ArrayList<>();

	Long vTransactionId = null;

	LogWriter logWriter;
	String sessionId;
	int virtualSessionId = 0;
	int sessionCount;
	long line;
	BigDecimal parseDur;
	BigDecimal bindDur;
	String query;
//	private long tempUsage = 0;
	private List<LogLine> bastirilan = new ArrayList<>();
//	bind;
//	private LogLine parse;
//	private LogLine command;
////	private int pbccPattern = 0;
	private String bindDetail;

	private Long virtualTransactionId = null;

	private boolean waitDuration = false;

	private LogLine statement;

	AtomicInteger sentLogCount = new AtomicInteger();

	public void process(LogLine ll) {
		ll.virtual_session_id = this.virtualSessionId;

		if (ll.virtual_transaction_id != null) {
			if (this.virtualTransactionId == null) {
				this.virtualTransactionId = ll.virtual_transaction_id;
			} else if (!ll.virtual_transaction_id.equals(this.virtualTransactionId)) {
				resetPbcc(false);
				this.virtualTransactionId = ll.virtual_transaction_id;
			}
		}

		if (ll.error_severity.equals("LOG") && ll.command_tag != null) {

			if (ll.command_tag.equals("DISCARD ALL")) {
				virtualSessionId++;
				resetPbcc(false);
				return;
			}

			if (ll.command_tag.equals("PARSE")) {
//				this.parse = ll;
				this.parseDur = ll.getDuration();
				this.bastirilan.add(ll);
				return;
			} else if (ll.command_tag.equals("BIND")) {
//				this.bind = ll;
				if (ll.detail.startsWith("parameters:")) {
					bindDetail = ll.detail.substring("parameters:".length() + 1);
				}
				this.bindDur = ll.getDuration();
				this.bastirilan.add(ll);
//				this.bindDetail = ll.detail;
				return;
			}

			if (ll.message != null) {
				if (ll.isStatement()) {
					this.statement = ll;
					return;
				} else if (statement != null && ll.message.startsWith("temporary file:")) {
					int ind = ll.message.indexOf("size");
					if (ind > 0) {
						String szs = ll.message.substring(ind + 4).trim();
						try {
							long k = Long.parseLong(szs);
							statement.increaseTempUsage(k);
							return;
						} catch (Exception p) {
							logger.error("Failed to extract size of temp file:" + szs, p);
						}
					}
				} else if (ll.message.startsWith("duration:")) {
					if (this.statement != null) {
						statement.update(bindDur, parseDur, ll.getDuration(), bindDetail, ll.command_tag);
						resetPbcc(true);
						return;
					}
				}
			}

		}

		writeLog(ll, true);
	}

	public void resetPbcc(boolean suc) {
		this.waitDuration = false;

		if (suc || statement != null) {
			writeLog(statement, false);
		} else if (!suc) {
			for (LogLine logLine : bastirilan) {
				writeLog(logLine, true);
			}
		}
		this.parseDur = null;
		this.bindDur = null;
		this.bindDetail = null;
		this.statement = null;
		if (this.bastirilan.size() > 0)
			this.bastirilan.clear();
	}

	private void writeLog(LogLine logLine, boolean canBeFiltered) {
		logWriter.write(logLine, canBeFiltered, sentLogCount);
	}

}
