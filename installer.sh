#!/bin/bash

if [ -d "target/pglogway" ]
	then
	rm -rf target/pglogway
fi

cp -a pglogway target/

cd target

echo "Copy jar to install directory"
cp pglogway-jar-with-dependencies.jar pglogway

echo "Creating package file target/pglogway-install.tar.gz"
tar -zcvf pglogway-install.tar.gz pglogway

cd ..

echo "Done"
