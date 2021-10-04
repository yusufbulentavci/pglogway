#!/bin/bash

yum install java-11-openjdk-headless.x86_64

adduser --system --shell=/bin/bash pglogway || echo "Failed to create pglogway user, exiting"; exit 1;
echo "pglogway user created"
usermod -a -G postgres pglogway || echo "Failed to add pglogway user to postgres group, exiting"; exit 1;
echo "pglogway user added to postgres group"

echo "!!Do not forget to give read permission to group for postgresql log directories; chmod g+r /pgdata/logX"

    1  ssh-keygen -t rsa
    2  ssh-copy-id 10.150.151.229


  367  chmod -R g+rwx /pgdata/log

	
if [ -f "/etc/pglogway.ini" ]
	then
		echo "/etc/pglogway.ini file exists, not modified"
	else
		cp pglogway.ini /etc/
		chown pglogway:pglogway /etc/pglogway.ini
		echo "/etc/pglogway.ini file created"
fi

if [ -f "/etc/pglogway-log4j2.properties" ]
	then
		echo "/etc/pglogway-log4j2.properties file exists, not modified"
	else
		cp pglogway-log4j2.properties /etc/
		chown pglogway:pglogway /etc/pglogway-log4j2.properties
		echo "/etc/pglogway-log4j2.properties file created"
fi

if [ -d "/opt/pglogway" ]
	then
		rm -rf /opt/pglogway
		echo "/opt/pglogway directory exist, deleted"
fi

mkdir /opt/pglogway
echo "/opt/pglogway directory created"

cp pglogway-jar-with-dependencies.jar /opt/pglogway
cp uninstall.sh /opt/pglogway
chown -R pglogway:pglogway /opt/pglogway

if [ ! -d "/var/log/pglogway" ]
	then
		mkdir /var/log/pglogway
		chown pglogway:pglogway /var/log/pglogway
fi


cp pglogway.service /lib/systemd/system/

systemctl daemon-reload

echo "Installation completed"
