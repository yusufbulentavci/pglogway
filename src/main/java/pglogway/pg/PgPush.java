package pglogway.pg;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import pglogway.ConfDir;
import pglogway.DataSourceCon;
import pglogway.LogLine;
import pglogway.exceptions.CantFindNextDayException;
import pglogway.exceptions.ConfigException;
import pglogway.exceptions.FlushException;
import pglogway.exceptions.UnexpectedSituationException;

public class PgPush {

	static final Logger logger = LogManager.getLogger(PgPush.class);
	private DataSourceCon pgCon;
	private Connection con;

	private int pushed = 0;
	private int sent = 0;
	private String defaultTableName;
	private List<LogLine> bulked = new ArrayList<>();
	private String insertSql;
	private String hostname;
	private String cluster;
	private String month;
	private String day;
	private String year;
	private boolean checkExpiredIndexes;
	private int expireDays;
//	private String rangeFrom;
//	private String rangeTo;

	public PgPush(ConfDir confDir, String year, String month, String day, int hour, int expireDays)
			throws UnknownHostException, CantFindNextDayException {
		pgCon = confDir.getPpCon();
		this.hostname = ConfDir.getHostName();
		this.month = month;
		this.day = day;
		this.year = year;
		this.defaultTableName = "log_" + year + "_" + month + "_" + day;
		this.expireDays = expireDays;

		StringBuilder sb = new StringBuilder("insert into pgdaylog values (");
		for (int i = 0; i < 38; i++)
			sb.append("?,");
		sb.append("?)");
		this.cluster = confDir.getCluster();

		insertSql = sb.toString();
		if (logger.isDebugEnabled())
			logger.debug("PgPush is initialized");
	}

	public void connect() throws ConfigException, UnexpectedSituationException {
		try {
			Class dbDriver = Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			logger.error("Failed to find jdbc driver org.postgresql.Driver", e1);
			throw new ConfigException("Failed to find jdbc driver org.postgresql.Driver", e1);
		}
		String jdbcUrl = "jdbc:postgresql://" + pgCon.getHost() + ":" + pgCon.getPort() + "/pglogway";
		if (logger.isDebugEnabled())
			logger.debug("PgPush is connecting");
		try {
			this.con = DriverManager.getConnection(jdbcUrl, pgCon.getUser(), pgCon.getPwd());
			con.setAutoCommit(true);
			createTable();
			con.setAutoCommit(false);
		} catch (SQLException e) {
			logger.error("Failed to connect " + jdbcUrl, e);
			throw new ConfigException("Failed to find jdbc driver org.postgresql.Driver", e);
		}
	}

	private void createTable() throws ConfigException, UnexpectedSituationException {
		if (logger.isDebugEnabled())
			logger.debug("PgPush create table");
		if (con == null) {
			if (logger.isDebugEnabled())
				logger.debug("PgPush create table/con is null");
			throw new UnexpectedSituationException("PgPush create table/con is null");
		}

		try {
			String tableSql = IOUtils.toString(this.getClass().getResourceAsStream("/table.sql"), "UTF-8");
			if (logger.isDebugEnabled())
				logger.debug(tableSql);
			try (PreparedStatement statement = con.prepareStatement(tableSql)) {
				statement.execute();
			} catch (SQLException e) {
				if (logger.isDebugEnabled()) {
					logger.debug("Failed to create primary table with:" + tableSql);
				}
				// throw new RuntimeException(e);
			}
			if (logger.isDebugEnabled())
				logger.debug("PgPush create table/done");
		} catch (IOException e1) {
			logger.error("Failed to read resource table.sql");
			throw new ConfigException("Failed to read resource table.sql", e1);
		}

		int yi = Integer.parseInt(year);
		int mi = Integer.parseInt(month);
		int di = Integer.parseInt(day);
		LocalDate date = LocalDate.of(yi, mi, di);
//		LocalDate today = nextDay(year, month, day);

		createPartition(date.plusDays(-1));
		createPartition(date);
		createPartition(date.plusDays(1));
		getExpiredTables();

	}

	private void getExpiredTables() {
		if (expireDays <= 0) {
			return;
		}
		Calendar oldday = Calendar.getInstance(); // today
		oldday.add(Calendar.DAY_OF_YEAR, -1 * expireDays);
		
	}

//	select tablename from pg_tables where schemaname='public' and tablename like 'log_2%' and tablename<'log_2022_10_9';
	private void createPartition(LocalDate d) {
		int y = d.getYear();
		int m = d.getMonthValue();
		int day = d.getDayOfMonth();
		String tableName = tableName(y, m, day);

		String rangeFrom = d.getYear() + "-" + d.getMonthValue() + "-" + d.getDayOfMonth();
		LocalDate l = d.plusDays(1);
		String rangeTo = l.getYear() + "-" + l.getMonthValue() + "-" + l.getDayOfMonth();
//		if (logger.isDebugEnabled())
//			logger.debug("PgPush create partition table");
		String sql = "create unlogged table if not exists " + tableName + " partition of pgdaylog"
				+ " for values from ('" + rangeFrom + "')" + " to ('" + rangeTo + "');";
		try (PreparedStatement statement = con.prepareStatement(sql)) {
			statement.execute();
			if (logger.isDebugEnabled())
				logger.debug("PgPush create partition table/done");
		} catch (SQLException e) {
			logger.debug("Failed to create table with:" + sql);
			// throw new RuntimeException(e);
		}
	}

	private String tableName(int y, int m, int day) {
		String tableName = "log_" + y + "_" + m + "_" + day;
		return tableName;
	}

	public void push(LogLine ll) throws FlushException {
//		if (logger.isDebugEnabled()) {
//			logger.debug("Pushed:" + ll.toString());
//		}
		bulked.add(ll);
		if (bulked.size() > 10000) {
			flush();
		}
	}

	public void close() {
		try {
			if (con != null)
				con.close();
		} catch (SQLException e) {
		}
	}

	public void flush() throws FlushException {
		if (logger.isDebugEnabled())
			logger.debug("PgPush flush");
		try {
			con.setAutoCommit(false);
		} catch (SQLException e1) {
			logger.error("Failed to set autocommit", e1);
		}
		try (PreparedStatement ps = con.prepareStatement(this.insertSql)) {
			for (LogLine ll : bulked) {
				fill(ps, ll);
				ps.addBatch();
			}
			if (logger.isDebugEnabled())
				logger.debug("PgPush flush2");
			ps.executeBatch();
			if (logger.isDebugEnabled())
				logger.debug("PgPush flush3");
			con.commit();
			if (logger.isDebugEnabled())
				logger.debug("PgPush flush4");
			this.bulked.clear();
			if (logger.isDebugEnabled())
				logger.debug("PgPush flush5");
		} catch (SQLException e) {
			logger.error("Failed to push to pg", e);
			throw new FlushException(e);
		}
		if (logger.isDebugEnabled())
			logger.debug("PgPush flush-out");

		if (checkExpiredIndexes) {
			expireIndexes();
		}
	}

	private void expireIndexes() {
		if (logger.isDebugEnabled()) {
			logger.debug("Check expire indexes for index:" + this.indexName);
		}
		this.checkExpiredIndexes = false;
		if (expireDays <= 0)
			return;

		try (PreparedStatement ps = con.prepareStatement("")) {
			Response response;
			Request r = new Request("DELETE", toDel);
			response = restClient.performRequest(r);
			if (response.getStatusLine().getStatusCode() != 200) {
				logger.info("Index deleted:" + toDel);
				return;
			}
		} catch (IOException e) {
		}

		Calendar oldday = Calendar.getInstance(); // today
		oldday.add(Calendar.DAY_OF_YEAR, -1 * expireDays);

		for (int i = 0; i < 30; i++) {
			oldday.add(Calendar.DAY_OF_YEAR, -1);
			Integer year = oldday.get(Calendar.YEAR);
			Integer month = oldday.get(Calendar.MONTH);
			Integer day = oldday.get(Calendar.DAY_OF_MONTH);
			String toDel = tableName(year, month, day);
			deleteTable(toDel);
		}
	}

	private void deleteTable(String toDel) {
		if (logger.isDebugEnabled())
			logger.debug("Check index to delete:" + toDel);

		try (Prepared) {
			Response response;
			Request r = new Request("DELETE", toDel);
			response = restClient.performRequest(r);
			if (response.getStatusLine().getStatusCode() != 200) {
				logger.info("Index deleted:" + toDel);
				return;
			}
		} catch (IOException e) {
		}
	}

	private void fill(PreparedStatement ps, LogLine ll) throws SQLException {
		// log_time timestamp not null,
		ps.setTimestamp(1, new java.sql.Timestamp((long) ll.log_time * 1000));
		// cluster
		ps.setString(2, cluster);
		// host_name text not null,
		ps.setString(3, this.hostname);
		// user_name text,
		ps.setString(4, ll.user_name);
		// db_name text,
		ps.setString(5, ll.database_name);
		// pg_port int,
		psSetInt(ps, 6, ll.pgPort);

		// locker int,
		psSetInt(ps, 7, ll.locker);
		// locked int[],
		if (ll.locked != null) {
			Array array = con.createArrayOf("int", ll.locked);
			ps.setArray(8, array);
		} else {
			ps.setNull(8, Types.ARRAY);
		}
		// pid int,
		psSetInt(ps, 9, ll.process_id);
		// unix_socket boolean,
		psSetBoolean(ps, 10, ll.unix_socket);
		// client_ip text,
		ps.setString(11, ll.connection_from);
		// client_port int,
		psSetInt(ps, 12, null);
		// 12
		// sid text,
		ps.setString(13, ll.session_id);
		// vsid int,
		psSetInt(ps, 14, ll.virtual_session_id);
		// session_line_num bigint,
		psSetLong(ps, 15, ll.session_line_num);
		// command_tag text,
		ps.setString(16, ll.command_tag);
		// session_start_time timestamp,
		psSetTimestamp(ps, 17, ll.session_start_time);
		// tid bigint,
		psSetLong(ps, 18, ll.transaction_id);
		// vtid bigint,
		psSetLong(ps, 19, ll.virtual_transaction_id);
		// eseverity text,
		ps.setString(20, ll.error_severity);
		// sql_state_code text,
		ps.setString(21, ll.sql_state_code);
		// message text,
		ps.setString(22, ll.message);
		// detail text,
		ps.setString(23, ll.detail);
		// hint text,
		ps.setString(24, ll.hint);
		// internal_query text,
		ps.setString(25, ll.internal_query);
		// internal_query_pos int,
		psSetInt(ps, 26, ll.internal_query_pos);
		// context text,
		ps.setString(27, ll.context);
		// query text,
		ps.setString(28, ll.query);
		// query_hash int,
		psSetInt(ps, 29, ll.query_hash);
		// query_pos int,
		psSetInt(ps, 30, ll.query_pos);
		// location text,
		ps.setString(31, ll.location);
		// application_name text,
		ps.setString(32, ll.application_name);
		// duration double,
		psSetDouble(ps, 33, ll.getduration());
		// bind_duration double,
		psSetDouble(ps, 34, ll.getbind_duration());
		// parse_duration double,
		psSetDouble(ps, 35, ll.getparse_duration());
		// parameters text,
		ps.setString(36, ll.bindDetail);
		// temp_usage bigint,
		psSetLong(ps, 37, ll.gettemp_file_size());
		// csv_ind int,
		psSetInt(ps, 38, ll.csvInd);
		// csv text
		ps.setString(39, ll.csv);

	}

	private void psSetDouble(PreparedStatement ps, int i, Double val) throws SQLException {
		if (val == null) {
			ps.setNull(i, Types.DOUBLE);
		} else {
			ps.setDouble(i, val);
		}
	}

	private void psSetTimestamp(PreparedStatement ps, int i, Long val) throws SQLException {
		if (val == null) {
			ps.setNull(i, Types.TIMESTAMP);
		} else {
			ps.setTimestamp(i, new java.sql.Timestamp((long) val * 1000));
		}
	}

	private void psSetLong(PreparedStatement ps, int i, Long val) throws SQLException {
		if (val == null) {
			ps.setNull(i, Types.BIGINT);
		} else {
			ps.setLong(i, val);
		}
	}

	private void psSetBoolean(PreparedStatement ps, int i, Boolean val) throws SQLException {
		if (val == null) {
			ps.setNull(i, Types.BOOLEAN);
		} else {
			ps.setBoolean(i, val);
		}
	}

	private void psSetInt(PreparedStatement ps, int i, Integer val) throws SQLException {
		if (val == null) {
			ps.setNull(i, Types.INTEGER);
		} else {
			ps.setInt(i, val);
		}
	}

	public void checkExpiredIndexes() {
		this.checkExpiredIndexes = true;
	}

	public int getPushed() {
		return pushed;
	}

}
