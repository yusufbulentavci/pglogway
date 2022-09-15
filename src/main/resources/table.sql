create unlogged table if not exists hostdaylog (	hostday text not null,
	host_name text not null,
	log_time timestamp not null,
	user_name text,
	db_name text,
	pg_port int,
	locker int,
	locked int[],
	pid int,
	unix_socket boolean,
	client_ip text,
	client_port int,
	sid text,
	vsid int,
	session_line_num bigint,
	command_tag text,
	session_start_time timestamp,
	tid bigint,
	vtid bigint,
	eseverity text,
	sql_state_code text,
	message text,
	detail text,
	hint text,
	internal_query text,
	internal_query_pos int,
	context text,
	query text,
	query_hash int,
	query_pos int,
	location text,
	application_name text,
	duration numeric,
	bind_duration numeric,
	parse_duration numeric,
	parameters text,
	temp_usage bigint,
	csv_ind int,
	csv text,
	cluster text
) partition by list(hostday);