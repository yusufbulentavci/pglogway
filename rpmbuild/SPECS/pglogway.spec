Name:           pglogway
Version:        2.5.0
Release:        1%{?dist}
Summary:        Postgresql log i) compress and send logs to backup machine. ii) tail,filter,merge and send to elastic stack
License:        Apache-2.0
URL:            https://github.com/yusufbulentavci/pglogway
BuildRoot: /tmp/pglogway


Requires: java-11-openjdk-headless
#Requires:  /bin/java
Requires(pre): /usr/sbin/useradd, /usr/bin/getent
Requires(postun): /usr/sbin/userdel

%description
Postgresql log i) compress and send logs to backup machine. ii) tail,filter,merge and send to elastic stack

%pre
/usr/bin/getent passwd pglogway || /usr/sbin/useradd -r --create-home -d /usr/local/share/pglogway -s /bin/bash pglogway
/usr/bin/getent group postgres && /sbin/usermod -a -G postgres pglogway


%post
systemctl daemon-reload

%preun
if [ $1 == 0 ]; then #uninstall
  systemctl unmask %{name}.service
  systemctl stop %{name}.service
  systemctl disable %{name}.service
fi

%postun
/usr/sbin/userdel pglogway
if [ $1 == 0 ]; then #uninstall
  systemctl daemon-reload
  systemctl reset-failed
fi

%prep
mkdir -p $RPM_BUILD_ROOT/etc
cp -f /home/rompg/workspace/pglogway/pglogway/pglogway.ini $RPM_BUILD_ROOT/etc
cp -f /home/rompg/workspace/pglogway/pglogway/pglogway-log4j2.properties $RPM_BUILD_ROOT/etc

mkdir -p $RPM_BUILD_ROOT/usr/local/share/pglogway
cp -f /home/rompg/workspace/pglogway/target/pglogway-jar-with-dependencies.jar $RPM_BUILD_ROOT/usr/local/share/pglogway

mkdir -p $RPM_BUILD_ROOT/lib/systemd/system/
cp -f /home/rompg/workspace/pglogway/pglogway/pglogway.service $RPM_BUILD_ROOT/lib/systemd/system/

mkdir -p $RPM_BUILD_ROOT/var/log/pglogway
exit

%build

%install

%files
%attr(0644, pglogway, pglogway) /etc/pglogway.ini
%attr(0644, pglogway, pglogway) /etc/pglogway-log4j2.properties
%attr(0644, pglogway, pglogway) /usr/local/share/pglogway/pglogway-jar-with-dependencies.jar
%attr(0644, root, root) /lib/systemd/system/pglogway.service
%attr(0644, pglogway, pglogway) /var/log/pglogway

