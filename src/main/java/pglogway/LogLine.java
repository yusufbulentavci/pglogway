package pglogway;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonGetter;

//2022-07-21 09:47:27.096 +03,"postgres","sina",1109630,"[local]",62d8f549.10ee7e,3,"idle",2022-07-21 09:42:17 +03,42/1723265,0,LOG,00000,"statement: create table sinadene( int a);",,,,,,,,"exec_simple_query, postgres.c:1045","psql"
//2022-07-21 09:47:27.106 +03,"postgres","sina",1109630,"[local]",62d8f549.10ee7e,4,"CREATE TABLE",2022-07-21 09:42:17 +03,42/1723265,0,ERROR,42704,"type ""a"" does not exist",,,,,,"create table sinadene( int a);",28,"typenameType, parse_type.c:275","psql"
//2022-07-21 09:47:33.381 +03,"postgres","sina",1109630,"[local]",62d8f549.10ee7e,5,"idle",2022-07-21 09:42:17 +03,42/1723266,0,LOG,00000,"statement: create table sinadene( a int);",,,,,,,,"exec_simple_query, postgres.c:1045","psql"
//2022-07-21 09:47:49.950 +03,"postgres","sina",1109630,"[local]",62d8f549.10ee7e,7,"idle",2022-07-21 09:42:17 +03,42/1723267,0,LOG,00000,"statement: alter table sinadene add column b int;",,,,,,,,"exec_simple_query, postgres.c:1045","psql"
//2022-07-21 09:00:22.344 +03,"bs_rw","bs",1107680,"10.150.151.154:60936",62d8ea15.10e6e0,3871,"INSERT",2022-07-21 08:54:29 +03,22/8027705,22309712,LOG,00000,"execute S_1: INSERT INTO logging_event_property (event_id, mapped_key, mapped_value) VALUES ($1, $2, $3)","parameters: $1 = '142520', $2 = 'spring.datasource.password', $3 = '376Rs4xg'",,,,,,,"exec_execute_message, postgres.c:2065","PostgreSQL JDBC Driver"
public class LogLine {
	static final Logger logger = LogManager.getLogger(LogLine.class.getName());

	@JsonGetter("host.name")
	public String getHostName() {
		return ConfDir.getHostName();
	}

	Long log_time;

	@JsonGetter("@timestamp")
	public Long getTimestamp() {
		return log_time;
	}

	String user_name;

	@JsonGetter("postgresql.log.user")
	public String getUserName() {
		return user_name;
	}

	String database_name;

	@JsonGetter("postgresql.log.database")
	public String getDatabaseName() {
		return database_name;
	}

	final Integer pgPort;

	@JsonGetter("postgresql.log.port")
	public Integer getPort() {
		return pgPort;
	}

	Integer locker;

	@JsonGetter("postgresql.log.locker")
	public Integer getLocker() {
		return locker;
	}

	Integer[] locked;

	@JsonGetter("postgresql.log.locked")
	public Integer[] getLocked() {
		return locked;
	}

	Integer process_id;

	@JsonGetter("process.pid")
	public Integer getPid() {
		return this.process_id;
	}

	Boolean unix_socket;

	@JsonGetter("unix_socket")
	public Boolean getUnixSocket() {
		return this.unix_socket;
	}

	String connection_from;

	@JsonGetter("client.ip")
	public String getConnectionFrom() {
		return connection_from;
	}

	String connection_from_port;

	@JsonGetter("client.port")
	public String getConnectionFromPort() {
		return connection_from_port;
	}

	String session_id;

	@JsonGetter("session_id")
	public String getSession_id() {
		return session_id;
	}

	Long session_line_num;

	@JsonGetter("session_line_num")
	public Long getSessionLineNum() {
		return this.session_line_num;
	}

	String command_tag; // BEGIN, SET, COMMIT, PARSE, DISCARD ALL, SELECT, UPDATE, INSERT, idle

	@JsonGetter("command_tag")
	public String getCommand_tag() {
		return command_tag;
	}

	Long session_start_time;

	@JsonGetter("session_start_time")
	public Long getSession_start_time() {
		return session_start_time;
	}

	Long transaction_id;

	@JsonGetter("transaction_id")
	public Long getTransaction_id() {
		return transaction_id;
	}

	Long virtual_transaction_id;

	@JsonGetter("virtual_transaction_id")
	public Long getVirtual_transaction_id() {
		return virtual_transaction_id;
	}

	String error_severity; // DEBUG, LOG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC, ???

	@JsonGetter("postgresql.log.level")
	public String getError_severity() {
		return error_severity;
	}

	String sql_state_code;

	@JsonGetter("sql_state_code")
	public String getSql_state_code() {
		return sql_state_code;
	}

	String message;

	@JsonGetter("postgresql.log.message")
	public String message() {
		return message;
	}

	String detail;

	@JsonGetter("detail")
	public String detail() {
		return detail;
	}

	String hint;

	@JsonGetter("hint")
	public String hint() {
		return hint;
	}

	String internal_query;

	@JsonGetter("internal_query")
	public String internal_query() {
		return internal_query;
	}

	Integer internal_query_pos;

	@JsonGetter("internal_query_pos")
	public Integer getInternal_query_pos() {
		return internal_query_pos;
	}

	String context;

	@JsonGetter("context")
	public String getcontext() {
		return context;
	}

	String query;

	@JsonGetter("query")
	public String getquery() {
		return query;
	}

	Integer query_pos;

	@JsonGetter("query_pos")
	public Integer getquery_pos() {
		return query_pos;
	}

	String location;

	@JsonGetter("location")
	public String getlocation() {
		return location;
	}

	String application_name;

	@JsonGetter("application_name")
	public String getapplication_name() {
		return application_name;
	}

	private BigDecimal duration;

	@JsonGetter("duration")
	public Double getduration() {
		if (duration == null)
			return null;
		return duration.doubleValue();
	}

	private BigDecimal bindDur;

	@JsonGetter("bind_duration")
	public Double getbind_duration() {
		if (bindDur == null)
			return null;
		return bindDur.doubleValue();
	}

	private BigDecimal parseDur;

	@JsonGetter("parse_duration")
	public Double getparse_duration() {
		if (parseDur == null)
			return null;
		return parseDur.doubleValue();
	}

	String bindDetail;

	@JsonGetter("parameters")
	public String getbindDetail() {
		return bindDetail;
	}

	Integer virtual_session_id;

	@JsonGetter("virtual_session_id")
	public Integer getvirtual_session_id() {
		return virtual_session_id;
	}

	private Long tempUsage = null;

	@JsonGetter("temp_file_size")
	public Long gettemp_file_size() {
		if (tempUsage == null)
			return null;
		return tempUsage / (1024 * 1024);
	}

	Integer csvInd;

	@JsonGetter("csv_ind")
	public Integer getcsvInd() {
		return csvInd;
	}

	String csv;

	@JsonGetter("csv")
	public String getcsv() {
		return this.csv;
	}

	private DateTimeFormatter formatter;

	private boolean isStatement = false;

	public LogLine(DateTimeFormatter formatter, Integer csvInd, String error_severity, String command_tag,
			String message, Integer pgPort) {
		this.formatter = formatter;
		this.csvInd = csvInd;
		this.error_severity = error_severity;
		this.command_tag = command_tag;
		this.message = message;
		this.pgPort = pgPort;
	}

	// 2021-02-08 10:31:38.693
	// yyyy-MM-dd HH:mm:ss.SSS
//	static SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	// Feb 10, 2021 @ 14:43:57.058
	// MMM dd, YYYY @ HH:mm:ss.SSS
	// YYYY-MM-ddTHH:mm:ss.SSSZ
//	static SimpleDateFormat gener =
//			new SimpleDateFormat("YYYY-MM-ddTHH:mm:ss.SSSZ", Locale.US);
	public LogLine(DateTimeFormatter formatter, int csvInd, String[] record, Integer pgPort) {
		this.formatter = formatter;
		this.pgPort = pgPort;
		this.csvInd = csvInd;
		{
			this.log_time = parseTime(record[0]);
		}
//		String llmen = nul(record[0]+" "+record[1));
//		try {
////			Date parsedDate = parser.parse(llmen);
////			log_time=gener.format(parsedDate);
//		} catch (ParseException e) {
//			logger.error("Failed to parse, regen date:"+llmen, e);
//		}

		user_name = nul(record[1]);
		database_name = nul(record[2]);
		process_id = parseInt(record[3]);
		connection_from = nul(record[4]);
		if (connection_from != null) {
			if (connection_from.equals("[local]")) {
				unix_socket = true;
			} else {
				String[] dd = connection_from.split(":");
				connection_from = dd[0];
				if (dd.length > 1) {
					connection_from_port = dd[1];
				} else {
					connection_from_port = "0";
				}
			}
		}
		session_id = nul(record[5]);
		session_line_num = parseLong(record[6]);
		command_tag = nul(record[7]);
		session_start_time = parseTime(record[8]);
		virtual_transaction_id = parseVirtualTid(nul(record[9]));
		transaction_id = parseLong(record[10]);
		error_severity = nul(record[11]);
		sql_state_code = nul(record[12]);
		message = nul(record[13]);
		detail = nul(record[14]);
		hint = nul(record[15]);
		internal_query = nul(record[16]);
		internal_query_pos = parseInt(record[17]);
		context = nul(record[18]);
		query = nul(record[19]);
		query_pos = parseInt(record[20]);
		location = nul(record[21]);
		application_name = nul(record[22]);

		if (message != null) {
			if (message.startsWith("duration:")) {
				this.duration = parseDuration(message);
			}

			if (command_tag == null) {
				// parse checkpoint
				// postgresql.log.message
				// message// checkpoint complete: wrote 1 buffers (0.0%); 0 WAL file(s) added, 0
				// removed, 0 recycled; write=0.109 s, sync=0.001 s, total=0.118 s; sync
				// files=1, longest=0.001 s, average=0.001 s; distance=3 kB, estimate=40 kB
			}
//			message.indexOf(" )
			// System.out.println(message+"===>"+duration);

			if (query == null && this.message != null) {
				if (this.message.startsWith("statement") || this.message.startsWith("execute")) {
					int ind = this.message.indexOf(":");
					if (ind > 0) {
						this.query = this.message.substring(ind + 1).trim();
						this.message = this.message.replace(this.query, "--query--");
						this.isStatement = true;
					}
				}
			}
		}
		if (detail != null) {
			if (detail.startsWith("Process holding the lock:")) {
				resolveDetailLock();
			}
		}
	}

	private Long parseVirtualTid(String n) {
		if (n == null)
			return null;
		int ind = n.indexOf('/');
		if (ind < 0)
			return null;
		n = n.substring(ind + 1);
		try {
			Long l = Long.parseLong(n);
			if (l == 0)
				return null;
			return l;
		} catch (Exception e) {
			logger.error("Can not parse long: n", e);
			return null;
		}

	}

	private void resolveDetailLock() {
		try {
//			Process holding the lock: 3297010. Wait queue: .
//			String s = "Process holding the lock: 2957. Wait queue: 43571, 154886, 3031, 152813, 154889, 154909, 154885, 31167, 65272, 65246, 3014, 43550, 3012";
			String d1 = detail.substring("Process holding the lock:".length());
//			System.out.println(d1);
			int dot = d1.indexOf('.');
			String phl = d1.substring(0, dot);
//			System.out.println(phl);
			this.locker = Integer.parseInt(phl.trim());
//			System.out.println(locker);
			String wq = d1.substring(dot + 1).trim();
			String rest = wq.substring("Wait queue:".length());
			rest = rest.replace('.', ' ');
			String[] ws = rest.split(",");
			this.locked = new Integer[ws.length];
			int i = 0;
			for (String string : ws) {
				string = string.trim();
				if (string.length() == 0 || string.equals("."))
					continue;
				locked[i++] = Integer.parseInt(string.trim());
			}
//			System.out.println(locked.toString());
		} catch (Exception e) {
			logger.error("Failed to parse detail lock:" + detail);
		}
	}
//
//	public static void main(String[] args) {
//		try {
////			String s = "Process holding the lock: 2957. Wait queue: 43571, 154886, 3031, 152813, 154889, 154909, 154885, 31167, 65272, 65246, 3014, 43550, 3012";
////			String s = "Process holding the lock: 3297010. Wait queue: .";
////			//			String s = "Process holding the lock: 3295837. Wait queue: 3295785.";
////
////			String d1 = s.substring("Process holding the lock:".length());
////			System.out.println(d1);
////			int dot = d1.indexOf('.');
////			if (dot < 0)
////				return;
////			String phl = d1.substring(0, dot);
////			System.out.println(phl);
////			Integer locker = Integer.parseInt(phl.trim());
////			System.out.println(locker);
////			String wq = d1.substring(dot + 1).trim();
////			String rest = wq.substring("Wait queue:".length());
////			rest=rest.replace('.', ' ');
////			JSONArray locked = new JSONArray();
////			String[] ws = rest.split(",");
////			for (String string : ws) {
////				string=string.trim();
////				if(string.length()==0 || string.equals("."))
////					continue;
////				locked.put(Integer.parseInt(string.trim()));
////			}
////			System.out.println(locked.toString());
//			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS] x");
//			String x = "2022-08-09 07:15:14.754 UTC";
//			if(x.endsWith("UTC")) {
//				x=x.substring(0, x.length()-3)+"+00";
//			}
//			System.out.println(x);
//			TemporalAccessor dateTime = formatter.parse(x);
//			
////			TemporalAccessor dateTime = formatter.parse("2022-08-09 07:15:14.754 +03");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

	private Long parseTime(String str) {
		if (str.endsWith("UTC")) {
			str = str.substring(0, str.length() - 3) + "+00";
		}
		TemporalAccessor dateTime = formatter.parse(str);
		return dateTime.getLong(java.time.temporal.ChronoField.INSTANT_SECONDS);
	}

	// duration: 0.153 ms
	private BigDecimal parseDuration(String smp) {
//		String smp = "\"message\":\"duration: 0.25 ms dsfafd";
		try {
			String a = smp.substring(10);
			int firstSpace = a.indexOf(' ');
			if (firstSpace < 0) {
				logger.error("Failed to parse duration1:" + smp);
				return null;
			}
			String time = a.substring(0, firstSpace);
			BigDecimal bt = new BigDecimal(time);
			bt.setScale(4);
			BigDecimal sec = null;
			String aa = a.substring(firstSpace + 1);
			if (aa.startsWith("ms")) {
				sec = durInSec(bt);
			} else if (aa.startsWith("s")) {
				sec = bt;
			} else {
				logger.error("Failed to parse duration2:" + smp);
				return null;
			}
			return sec;
		} catch (Exception e) {
			System.err.println(smp);
			e.printStackTrace();

			return null;
		}
	}

	private String nul(String string) {
		if (string == null || string.trim().length() == 0)
			return null;
		return string;
	}

	private Integer parseInt(String string) {
		if (string == null || string.trim().length() == 0)
			return null;
		try {
			return Integer.parseInt(string);
		} catch (Exception e) {
//			System.err.println(string);
//			return null;
			throw new RuntimeException(string);
		}
	}

	private Long parseLong(String string) {
		if (string == null)
			return null;
		return Long.parseLong(string);
	}

	public static void addFields(JSONObject p) {
		p.put("@timestamp", new JSONObject().put("type", "date").put("format", "YYYY-MM-ddTHH:mm:ss.SSSZ"));
	}

	JSONObject toJson(String fileName) {
		JSONObject ret = new JSONObject();

		ret.put("host.name", ConfDir.getHostName());

		if (log_time != null)
			ret.put("@timestamp", log_time);
		if (this.user_name != null)
			ret.put("postgresql.log.user", this.user_name);
		if (this.database_name != null)
			ret.put("postgresql.log.database", this.database_name);
		if (this.pgPort != null) {
			ret.put("postresql.log.port", this.pgPort);
		}
		if (this.locker != null) {
			ret.put("postresql.log.locker", locker);
		}
		if (this.locked != null) {
			ret.put("postresql.log.locked", locked);
		}
		if (this.process_id != null)
			ret.put("process.pid", this.process_id);

		if (this.unix_socket != null) {
			ret.put("unix_socket", unix_socket);
		} else {
			if (this.connection_from != null) {
				ret.put("client.ip", this.connection_from);
				ret.put("client.port", this.connection_from_port);
			}
		}

		if (this.session_id != null)
			ret.put("session_id", this.session_id);
		if (this.session_line_num != null)
			ret.put("session_line_num", this.session_line_num);
		if (this.command_tag != null)
			ret.put("command_tag", this.command_tag);
		if (this.session_start_time != null)
			ret.put("session_start_time", this.session_start_time);
		if (this.transaction_id != null)
			ret.put("transaction_id", this.transaction_id);
		if (this.virtual_transaction_id != null)
			ret.put("vt_id", this.virtual_transaction_id);
		if (this.error_severity != null)
			ret.put("postgresql.log.level", this.error_severity);
		if (this.sql_state_code != null)
			ret.put("sql_state_code", this.sql_state_code);
		if (this.message != null)
			ret.put("postgresql.log.message", this.message);
		if (this.detail != null)
			ret.put("detail", this.detail);
		if (this.hint != null)
			ret.put("hint", this.hint);
		if (this.internal_query != null)
			ret.put("internal_query", this.internal_query);
		if (this.internal_query_pos != null)
			ret.put("internal_query_pos", this.internal_query_pos);
		if (this.context != null)
			ret.put("context", this.context);
		if (this.query != null)
			ret.put("query", this.query);
		if (this.query_pos != null)
			ret.put("query_pos", this.query_pos);
		if (this.location != null)
			ret.put("location", this.location);
		if (this.application_name != null)
			ret.put("application_name", this.application_name);

		if (this.duration != null) {
//			System.out.println("->>>>"+duration.doubleValue());
			ret.put("duration", duration.doubleValue());
		}

		if (this.bindDur != null) {
			ret.put("bind_duration", bindDur.doubleValue());
		}
		if (this.getBindDetail() != null) {
			ret.put("parameters", this.getBindDetail());
		}
		if (this.parseDur != null) {
			ret.put("parse_duration", parseDur.doubleValue());
		}
		if (this.virtual_session_id != null) {
			ret.put("virtual_session_id", virtual_session_id);
		}

		if (this.tempUsage != null) {
			ret.put("temp_file_size", tempUsage / (1024 * 1024));
		}

		if (csvInd != null)
			ret.put("csv_ind", csvInd);

		ret.put("csv", fileName);

		return ret;
	}

	public void update(BigDecimal bind, BigDecimal parse, BigDecimal duration, String bindDetail) {
		this.bindDur = bind;
		this.parseDur = parse;
		if (duration == null) {
			duration = new BigDecimal(0);
			duration.setScale(4);
		}
		if (bindDur != null)
			duration.add(bindDur);
		if (parseDur != null)
			duration.add(parseDur);

		duration = durInSec(duration);

		this.bindDetail = bindDetail;
	}

	static BigDecimal bin = new BigDecimal(1000);
	static {
		bin.setScale(4);
	}

	protected BigDecimal durInSec(BigDecimal dur) {
		return dur.divide(bin);
	}

	public BigDecimal getDuration() {
		return duration;
	}

	@Override
	public String toString() {
		return "LogLine [log_time=" + log_time + ", user_name=" + user_name + ", database_name=" + database_name
				+ ", process_id=" + process_id + ", connection_from=" + connection_from + ", session_id=" + session_id
				+ ", session_line_num=" + session_line_num + ", command_tag=" + command_tag + ", session_start_time="
				+ session_start_time + ", virtual_transaction_id=" + virtual_transaction_id + ", transaction_id="
				+ transaction_id + ", error_severity=" + error_severity + ", sql_state_code=" + sql_state_code
				+ ", message=" + message + ", detail=" + detail + ", hint=" + hint + ", internal_query="
				+ internal_query + ", internal_query_pos=" + internal_query_pos + ", context=" + context + ", query="
				+ query + ", query_pos=" + query_pos + ", location=" + location + ", application_name="
				+ application_name + ", duration=" + duration + ", bindDur=" + bindDur + ", parseDur=" + parseDur
				+ ", virtual_session_id=" + virtual_session_id + ", csvInd=" + csvInd + ", unix_socket=" + unix_socket
				+ ", connection_from_port=" + connection_from_port + "]";
	}

//	public static void main(String[] args) throws ParseException {
//		String str = "2021-02-08 10:31:38.692 +03";
////		String str="2021-09-09 08:02:40 +03";
//		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS x");
//
////		TemporalAccessor dateTime = formatter.parse("2014-04-01 15:19:49.31146+05:30");
//		TemporalAccessor dateTime = formatter.parse(str);
//		System.out.println(dateTime.getLong(java.time.temporal.ChronoField.INSTANT_SECONDS));
//		//
////		Instant i=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss x").parse(str).toInstant();
////		long epochMillis = i.toEpochMilli();
////		
////		System.out.println(epochMillis);
////		
//
////		System.out.println(dateTime.toString());
//	}

	public Integer getPgPort() {
		return pgPort;
	}

	public String getBindDetail() {
		return bindDetail;
	}

	public void increaseTempUsage(long tempUsage2) {
		if (this.tempUsage == null) {
			this.tempUsage = 0l;
		}
		this.tempUsage += tempUsage2;
	}

	public String getQuery() {
		return query;
	}

	public boolean isStatement() {
		return isStatement;
	}

	public void setCsv(String jsonFileName) {
		this.csv = jsonFileName;
	}

}
