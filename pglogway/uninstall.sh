#!/bin/bash

systemctl stop pglogway
systemctl disable pglogway

rm -f /etc/pglogway.ini /etc/pglogway-log4j2.properties
	
rm -rf /usr/local/share/pglogway
rm -rf /var/log/pglogway


rm -f /lib/systemd/system/pglogway.service


systemctl daemon-reload

echo "Uninstallation completed"

