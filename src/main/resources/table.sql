create unlogged table if not exists pgdaylog (log_time timestamp not null,	
	cluster text,
	host_name text not null,
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
	csv text
) partition by range(log_time);
create index if not exists daylogtime_ind on pgdaylog(log_time);

create index if not exists pgdaylog_query_hash_idx on pgdaylog(query_hash);

create or replace function queryof(int) returns text
as $$ select query from pgdaylog where query_hash=$1 limit 1 $$
language sql;

