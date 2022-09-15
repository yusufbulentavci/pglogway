package pglogway;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PgPush {

	static final Logger logger = LogManager.getLogger(PgPush.class);
	private DataSourceCon pgCon;
	private Connection con;

	private int pushed = 0;
	private int sent = 0;
	private String key;
	private List<LogLine> bulked = new ArrayList<>();
	private String insertSql;
	private String hostname;
	private String cluster;

	public PgPush(ConfDir confDir, String year, String month, String day, int hour) {
		pgCon = confDir.getPpCon();
		this.hostname = ConfDir.getHostName();
		this.key = "log_" + hostname + "_" + year + "_" + month + "_" + day;
		StringBuilder sb = new StringBuilder("insert into " + key + "  values (");
		for (int i = 0; i < 39; i++)
			sb.append("?,");
		sb.append("?)");
		this.cluster = confDir.getCluster();

		insertSql = sb.toString();
		if (logger.isDebugEnabled())
			logger.debug("PgPush is initialized");
	}

	public void connect() {
		try {
			Class dbDriver = Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			logger.error("Failed to find jdbc driver org.postgresql.Driver", e1);
			throw new RuntimeException(e1);
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
			throw new RuntimeException(e);
		}
	}

	private void createTable() {
		if (logger.isDebugEnabled())
			logger.debug("PgPush create table");
		if (con == null) {
			if (logger.isDebugEnabled())
				logger.debug("PgPush create table/con is null");
			return;
		}

		try {
			String tableSql = IOUtils.toString(this.getClass().getResourceAsStream("/table.sql"), "UTF-8");
			if (logger.isDebugEnabled())
				logger.debug(tableSql);
			try (PreparedStatement statement = con.prepareStatement(tableSql)) {
				statement.execute();
			} catch (SQLException e) {
				logger.error("Failed to create primary table with:" + tableSql);
				throw new RuntimeException(e);
			}
			if (logger.isDebugEnabled())
				logger.debug("PgPush create table/done");
		} catch (IOException e1) {
			logger.error("Failed to read resource table.sql");
			throw new RuntimeException(e1);
		}

		if (logger.isDebugEnabled())
			logger.debug("PgPush create partition table");
		String sql = "create table if not exists " + key + "  partition of hostdaylog for values in ('" + key + "');";
		try (PreparedStatement statement = con.prepareStatement(sql)) {
			statement.execute();
			if (logger.isDebugEnabled())
				logger.debug("PgPush create partition table/done");
		} catch (SQLException e) {
			logger.error("Failed to create table with:" + sql);
			throw new RuntimeException(e);
		}
	}

	public void push(LogLine ll) {
		if (logger.isDebugEnabled()) {
			logger.debug("Pushed:" + ll.toString());
		}
		bulked.add(ll);
		if (bulked.size() > 100) {
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

	public void flush() {
		if (logger.isDebugEnabled())
			logger.debug("PgPush flush");
		try (PreparedStatement ps = con.prepareStatement(this.insertSql)) {
			for (LogLine ll : bulked) {
				fill(ps, ll);
				ps.addBatch();
			}
			ps.executeBatch();
			con.commit();
			this.bulked.clear();
		} catch (SQLException e) {
			logger.error("Failed to push to pg", e);
			throw new RuntimeException(e);
		}
	}

	private void fill(PreparedStatement ps, LogLine ll) throws SQLException {
		// hostday text
		ps.setString(1, this.key);
		// host_name text not null,
		ps.setString(2, this.hostname);
		// log_time timestamp not null,
		ps.setTimestamp(3, new java.sql.Timestamp((long) ll.log_time * 1000));
		// user_name text,
		if (ll.user_name != null)
			ps.setString(4, ll.user_name);
		// db_name text,
		if (ll.database_name != null)
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
		// cluster
		ps.setString(40, cluster);
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

	}

	public int getPushed() {
		return pushed;
	}

}
