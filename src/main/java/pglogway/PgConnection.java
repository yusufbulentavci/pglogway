package pglogway;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PgConnection {
	static final Logger logger = LogManager.getLogger(PgConnection.class.getName());

	public PgConnection(LogWriter logWriter, LogLine ll) {
		this.logWriter = logWriter;
		sessionId = ll.session_id;
	}

	List<LogLine> unknown = new ArrayList<>();

	LogWriter logWriter;
	String sessionId;
	int virtualSessionId = 0;
	int sessionCount;
	long line;
	BigDecimal parseDur;
	BigDecimal bindDur;
	String query;
	private long tempUsage = 0;
	private List<LogLine> bastirilan = new ArrayList<>();
//	bind;
//	private LogLine parse;
//	private LogLine command;
////	private int pbccPattern = 0;
	private String bindDetail;

	private String virtualTransactionId = null;

	private boolean waitDuration = false;

	private LogLine statement;

	public void process(LogLine ll) {
		ll.virtual_session_id = this.virtualSessionId;
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
					bindDetail = ll.detail.substring("parameters:".length()+1);
				}
				this.bindDur = ll.getDuration();
				this.bastirilan.add(ll);
//				this.bindDetail = ll.detail;
				return;
			}

			if (ll.message != null) {
				if (ll.getQuery() != null) {
					this.statement = ll;
					return;
				} else if (ll.message.startsWith("temporary file:")) {
					int ind = ll.message.indexOf("size");
					if (ind > 0) {
						String szs = ll.message.substring(ind + 4);
						try {
							int k = Integer.parseInt(szs);
							this.tempUsage += k;
							return;
						} catch (Exception p) {
							logger.error("Failed to extract size of temp file:" + szs, p);
						}
					}
				} else if (ll.message.startsWith("duration:")) {
					if (this.statement != null) {
						statement.update(bindDur, parseDur, ll.getDuration(), bindDetail);
						logWriter.write(statement);
						resetPbcc(true);
						return;
					}
				}
			}

//			else if (!waitDuration) {
//				this.command = ll;
//				this.waitDuration = true;
//				if (bindDetail == null && ll.getBindDetail() != null) {
//					bindDetail = ll.detail;
//				}
//				return;
//			} else if (waitDuration && command != null && ll.command_tag != null
//					&& ll.command_tag.equals(command.command_tag)) {
//				ll.updateDur(bind == null ? null : bind.getDuration(), parse == null ? null : parse.getDuration(),
//						command == null ? null : command.message, bindDetail);
//				if(this.tempUsage > 0) {
//					ll.setTempUsage(tempUsage);
//				}
//				resetPbcc(true);
//			} else {
//				resetPbcc(false);
//			}
		} else {
			resetPbcc(false);
		}

		logWriter.write(ll);
	}

	public void resetPbcc(boolean suc) {
		this.waitDuration = false;
		if (!suc) {
			for (LogLine logLine : bastirilan) {
				logWriter.write(logLine);
			}
		}
		this.parseDur = null;
		this.bindDur = null;
		this.bindDetail = null;
		if (this.bastirilan.size() > 0)
			this.bastirilan.clear();
		this.tempUsage = 0;
	}

}
