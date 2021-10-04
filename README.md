# pglogway
pglogway service for linux systems

- process postgresql logs in csv format
- convert csv lines into json and send it to elastic stack
- json namespace is compatible with elastic stack
- gzip and send log files to remote backup server
- rollover; keeps track of new csv file, rollover when needed
- online; tails csv file and process new csv lines
- duration; extract duration value as a json key
- summarizes; follow session and merges parse-bind-command-command csv line into one json line with extra duration parameters.
- virtual session; adds virtual session paramter, 'discard all' increases virtual session id
- telnet terminal to monitor
- service linux user is pglogway

# installation
- Only for linux systems
- Download and extract pglogway-install.tar.gz
- Run install.sh in pglogway directory
- Service is ready start, before starting let's configure
- Service runs with postgres user
    1  ssh-keygen -t rsa
    2  ssh-copy-id 10.150.151.229


  367  chmod -R g+rwx /pgdata/log


# configuration
In postgresql.conf
```
log_filename = 'postgresql-%Y-%m-%d_%H_%M_%S'
log_destination = 'csvlog'
log_line_prefix = '' 
log_duration = on
log_rotation_age = 1h
log_rotation_size = 0			
log_checkpoints = 'on'
log_connections = 'on'
log_disconnections = 'on'
log_lock_waits = 'on'
log_statement = 'all'
log_error_verbosity = 'verbose' 
log_file_mode = 0640


```

In /etc/pglogway.ini
```
[dir-1]
path=/xxx/log
```
# start
```
systemctl start pglogway.service
systemctl enable pglogway.service
```
# monitor
```
systemctl status pglogway.service
log file /var/log/pglogway/pglogway.log
```
# telnet
```
>telnet 127.0.0.1 2300
>status
>exit
```
# History
1.2.0
unix_socket added to json. If client_connection came as [local] client.ip will be null and unix_socket will be true

2.0.3
elasticsearch index mappings added
log4j->log4j2
2.1.0
Store management added
2.2.0
Filters added
Clean csvlog pair files
2.3.0
No elastic option added
2.4.0
duration parse duzeltildi
mesaja host.name, postgresql.log.port eklendi
linux kullanicisi artik pglogway
