#!/bin/bash

if id "postgres" &>/dev/null; then
    echo "postgres user exists"
else
	echo "postgres user not found. postgres required to be installed. pglogway installation failed, exiting"
	exit 1;
fi


javabin=''

if [ -f /etc/redhat-release ]; then
	javabin="/bin/java"
	if [ -f "/bin/java" ]; then
		yum install java-11-openjdk-headless.x86_64  || echo "Failed to install openjdk11, exiting"
	fi

else
	javabin="/usr/bin/java"
	if [ -f "/usr/bin/java" ]; then
		apt-get install openjdk-11-jre-headless || echo "Failed to install openjdk11, exiting"
	fi
fi


if [ $(getent group pglogway) ]; then
	echo "pglogway group exists"
else
	groupadd pglogway
	echo "pglogway group created"
fi

if id "pglogway" &>/dev/null; then
	echo "pglogway user found. no need to create"
else
	adduser --shell=/bin/bash -g pglogway -g postgres pglogway || (echo "Failed to create pglogway user, exiting"; exit 1)
	echo "pglogway user created"
fi

#echo "Adding user to pglogway and postgres groups"
#usermod -a -G postgres pglogway
#usermod -a -G pglogway pglogway


cp pglogway.ini pglogway-setup.ini
cp pglogway.service pglogway-setup.service

sed -i "s/PRM_PRM_JAVABIN/$javabin/" pglogway-setup.service


storemethod=''
#echo "!!Do not forget to give read permission to group for postgresql log directories; chmod g+r /pgdata/logX"
#    1  ssh-keygen -t rsa
#    2  ssh-copy-id 10.150.151.229

echo "pglogway loglari kopyalanacak mi?/Yes,y,Y,No,n,N"
while [[ $string != 'Yes' ]] && [[ $string != 'y' ]] && [[ $string != 'Y' ]]  && [[ $string != 'No' ]] && [[ $string != 'N' ]] && [[ $string != 'n' ]];
do
    read -p "Iceride pglogway loglari kopyalanacak mi?/Yes,y,Y,No,n,N " string # Ask the user to enter a string
done 

echo "gectik"

pgloghost=''
pgloghostdir=''

if [[ $string == 'Yes' ]] || [[ $string == 'y' ]] || [[ $string == 'Y' ]]; then
	storemethod='ssh'
	read -p "Loglarin kopyalanacagi host'un ip'sini giriniz.." pgloghost
	
	if [ ! -f /home/pglogway/.ssh/id_rsa.pub ]; then
		su - pglogway -c "ssh-keygen -t rsa" || (echo 'failed to genegare keypair, exiting pglogway installation'; exit 1)
	fi
	
	su - pglogway -c "ssh-copy-id $pgloghost" || (echo 'ssh key couldnt be copied. Pglogway installation failed, exiting'; exit 1)
	
	read -p "Loglarin kopyalanacagi hosttaki log ust dizinini giriniz:" pgloghostdir
	sed -i 's/PRM_STORE_HOST/$pgloghost/' pglogway-setup.ini
	sed -i 's/PRM_STORE_PATH/$pgloghostdir/' pglogway-setup.ini
else
	storemethod='remove'	
fi

sed -i "s/PRM_STORE_METHOD/$storemethod/" pglogway-setup.ini

string=''
while [[ $string != 'Yes' ]] && [[ $string != 'y' ]] && [[ $string != 'Y' ]]  && [[ $string != 'No' ]] && [[ $string != 'N' ]] && [[ $string != 'n' ]];
do
    read -p "loglar elastic'e atilacakmi?/Yes,y,Y,No,n,N " string # Ask the user to enter a string
done 

elastichost=''
elasticport=''
elasticuser=''
elasticpwd=''
if [[ $string == 'Yes' ]] || [[ $string == 'y' ]] || [[ $string == 'Y' ]]; then
	read -p "Elastic host'un ip'sini giriniz.." elastichost
	read -p "Elastic sunucu'un portunu giriniz.." elasticport
	read -p "Elastic kullanicisini giriniz.." elasticuser
	read -p "Elastic kullanicisinin sifresini giriniz.." elasticpwd
	
	sed -i 's/PRM_TO_ELASTIC/true/' pglogway-setup.ini
	
	sed -i "s/PRM_ELASTIC_HOST/$elastichost/" pglogway-setup.ini
	sed -i "s/PRM_ELASTIC_PORT/$elasticport/" pglogway-setup.ini
	sed -i "s/PRM_ELASTIC_USER/$elasticuser/" pglogway-setup.ini
	sed -i "s/PRM_ELASTIC_PWD/$elasticpwd/" pglogway-setup.ini
	
	echo 'Checking network access to ip and port'
	nc -zv -w 4 $elastichost $elasticport
	
	if [ $? -eq 0 ]; then 
		echo "Elastic server access check successful." 
	else
		echo "Elastic server access check failed. Installation canceled" 
	  	exit 1
	fi
else
	sed -i "s/PRM_TO_ELASTIC/false/" pglogway-setup.ini
	sed -i "s/PRM_ELASTIC_HOST/127.0.0.1/" pglogway-setup.ini
	sed -i "s/PRM_ELASTIC_PORT/9200/" pglogway-setup.ini
	sed -i "s/PRM_ELASTIC_USER/elastic/" pglogway-setup.ini
	sed -i "s/PRM_ELASTIC_USER_PWD/pwd/" pglogway-setup.ini
fi

echo "Log directory entry"
cluster=''
logdir=''
port=""
ind=0

if [[ $string == 'Yes' ]] || [[ $string == 'y' ]] || [[ $string == 'Y' ]]; then
	read -p "Enter cluster:" cluster
	read -p "Enter log directory:" logdir
	read -p "Enter postgresql server port:" port

	echo "[dir-$ind]"  >> pglogway-setup.ini
	echo "path=$logdir" >> pglogway-setup.ini
	echo "cluster=$cluster" >> pglogway-setup.ini
	echo "port=$port" >> pglogway-setup.ini
	
	chmod -R g+rwx $logdir

	while [[ $string != 'Yes' ]] && [[ $string != 'y' ]] && [[ $string != 'Y' ]]  && [[ $string != 'No' ]] && [[ $string != 'N' ]] && [[ $string != 'n' ]];
	do
	    read -p "Baska log dizini ekleyecek misiniz?/Yes,y,Y,No,n,N: " string
	done 
	ind=$((ind+1))
fi
	

if [ -f "/etc/pglogway.ini" ]; then
	echo "/etc/pglogway.ini file exists, not modified"
else
	cp pglogway-setup.ini /etc/pglogway.ini
	chown pglogway:pglogway /etc/pglogway.ini
	echo "/etc/pglogway.ini file created"
fi

if [ -f "/etc/pglogway-log4j2.properties" ]; then
	echo "/etc/pglogway-log4j2.properties file exists, not modified"
else
	cp pglogway-log4j2.properties /etc/
	chown pglogway:pglogway /etc/pglogway-log4j2.properties
	echo "/etc/pglogway-log4j2.properties file created"
fi

if [ -d "/usr/local/share/pglogway" ]; then
	rm -rf /usr/local/share/pglogway
	echo "/usr/local/share/pglogway directory exist, deleted"
fi

mkdir -p /usr/local/share/pglogway
echo "/usr/local/share/pglogway directory created"

cp pglogway-jar-with-dependencies.jar /usr/local/share/pglogway
cp uninstall.sh /usr/local/share/pglogway
chown -R pglogway:pglogway /usr/local/share/pglogway

if [ ! -d "/var/log/pglogway" ]; then
	mkdir /var/log/pglogway
	chown pglogway:pglogway /var/log/pglogway
fi

cp pglogway-setup.service /lib/systemd/system/pglogway.service

systemctl daemon-reload
echo "Installation completed"
echo "Please add following lines to postgresql configuration file and reload postgresql service"

cat << EOF
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
EOF



