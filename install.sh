#!/bin/bash
#
# Script name : install.sh
# Date        : 20/01/2015
# Author      : Jon Marshall
#
# Description:
#   Installs the zback python script, along with its dependencies
# 
# Usage example:
#   install.sh
#
##########################################################################
# Uncomment to debug
# export PS4='+${BASH_SOURCE}:${LINENO}:${FUNCNAME[0]}: '
#

ZDIR=$(pwd)

echo -e "\n"
echo "----------------------------------------------------------------------"
echo "                          Zback installer"
echo "----------------------------------------------------------------------"
echo -e "\n"

echo -e "\n"
echo "----------------------------------------------------------------------"
echo "                 Checking for python pre-requisites"
echo "----------------------------------------------------------------------"
echo -e "\n"

# Figure this may as well be done, doesn't take long and ensures the thing is
# actually installed without having to do laborious checks
apt-get update && apt-get install -y python python-setuptools pv mbuffer \
build-essential libssl-dev libffi-dev python-dev netcat-openbsd

echo -e "\n"
echo "----------------------------------------------------------------------"
echo "                       Beginning PIP install"
echo "----------------------------------------------------------------------"
echo -e "\n"

# Need to install this system-wide
easy_install pip

echo -e "\n"
echo "----------------------------------------------------------------------"
echo "                    Installing Python Virtualenv"
echo "----------------------------------------------------------------------"
echo -e "\n"

# This also needs to be system wide
pip install virtualenv

echo -e "\n"
echo "----------------------------------------------------------------------"
echo "             Installing zback dependencies in virtualenv"
echo "----------------------------------------------------------------------"
echo -e "\n"

cd $ZDIR && virtualenv env

$ZDIR/env/bin/pip install apscheduler python-daemon tabulate paramiko

echo -e "\n"
echo "----------------------------------------------------------------------"
echo "                        Running initial setup"
echo "----------------------------------------------------------------------"

#$ZDIR/env/bin/python $ZDIR/zback/configure.py 

echo -e "\n"
$ZDIR/env/bin/python setup.py install

echo -e "\n"
echo "----------------------------------------------------------------------"
echo "                        Creating local directories"
echo "----------------------------------------------------------------------"

mkdir $ZDIR/{log,run}

echo -e "\n"
echo "----------------------------------------------------------------------"
echo "                        Linking templates and scripts"
echo "----------------------------------------------------------------------"

[[ -d /etc/zabbix/zabbix_agentd.d/ ]] && ln -sv $ZDIR/conf/userparameter_zback.conf /etc/zabbix/zabbix_agentd.d/userparameter_zback.conf
cp -fv $ZDIR/conf/zback-client.service /lib/systemd/system/zback-client.service
cp -fv $ZDIR/conf/zback-server.service /lib/systemd/system/zback-server.service
ln -sv $ZDIR/bin/zback /usr/bin/zback

echo -e "\n"
echo "----------------------------------------------------------------------"
echo "                       	 Setup complete"
echo "----------------------------------------------------------------------"
echo -e "\n\n\n\n"

sleep 1
echo "Ensure you have setup offsite hosts and ssh config if you will be using
this - you will also need to seed them before offsite replication can commence.
For more details please see the wiki page relating to Zback usage."
echo -e "\n"
echo "Run the command zback edit to begin dataset setup"
