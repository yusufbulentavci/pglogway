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
	private LogLine bind;
	private LogLine parse;
	private LogLine command;
//	private int pbccPattern = 0;
	private String bindDetail;

	private String virtualTransactionId = null;

	private boolean pbccPattern = false;

	public void process(LogLine ll) {
		ll.virtual_session_id = this.virtualSessionId;
		if (ll.error_severity.equals("LOG") && ll.command_tag != null) {

			if (ll.command_tag.equals("DISCARD ALL")) {
				virtualSessionId++;
				resetPbcc(false);
				return;
			}

			if (ll.command_tag.equals("PARSE")) {
				this.parse = ll;
				return;
			} else if (ll.command_tag.equals("BIND")) {
				this.bind = ll;
				this.bindDetail = ll.detail;
				return;
			} else if (!pbccPattern) {
				this.command = ll;
				this.pbccPattern = true;
				if (bindDetail == null && ll.getBindDetail() != null) {
					bindDetail = ll.detail;
				}
				return;
			} else if (pbccPattern && command != null && ll.command_tag != null
					&& ll.command_tag.equals(command.command_tag)) {
				ll.updateDur(bind == null ? null : bind.getDuration(), parse == null ? null : parse.getDuration(),
						command == null ? null : command.message, bindDetail);
				resetPbcc(true);
			} else {
				resetPbcc(false);
			}
		} else {
			resetPbcc(false);
		}

		logWriter.write(ll);
	}

	public void resetPbcc(boolean suc) {
		this.pbccPattern = false;
		if (!suc) {
			if (bind != null) {
				logWriter.write(bind);
			}
			if (parse != null)
				logWriter.write(parse);
			if (command != null)
				logWriter.write(command);
		}
		this.parseDur = null;
		this.bindDur = null;
		this.bindDetail = null;
		this.bind = null;
		this.parse = null;
		this.command = null;
	}

}
