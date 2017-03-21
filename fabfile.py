"""
Python Fabric Fab File for Dumping Databases

To authenticate setup your user on the server, and configure your ~/.ssh/config to something like

### ~/.ssh/config begin ###

# Site
Host site.com IP.AD.DRE.SS
  ForwardAgent yes
  User USERNAME

Host *
  # Never ever do ForwardAgent to unknown hosts
  # https://blog.filippo.io/ssh-whoami-filippo-io/
  ForwardAgent no
  # Roaming through the OpenSSH client: CVE-2016-0777 and CVE-2016-0778
  UseRoaming no

### ~/.ssh/config end ###

### install fabric begin ###

http://www.fabfile.org/installing.html

#ubuntu
sudo apt-get -y remove python-paramiko
sudo apt-get install python-setuptools
sudo easy_install pip
sudo pip install --upgrade fabric==1.12.0 paramiko==1.17.2

$ pip list
Fabric (1.12.0)
paramiko (1.17.2)

### install fabric end ###

To view how to use any of these tasks, run `fab -l` in the directory where this script is located

Example:
# System Commands
fab -A -H [IP.AD.DRE.SS] -- "uptime"

# Generate the latest db dumps
time fab -A -H [IP.AD.DRE.SS] prep_db_dump

# Copy dumps to your laptop
scp -r [IP.AD.DRE.SS]:/tmp/db_export/ /path/to/destination/folder/


"""
# imports
from __future__ import with_statement
from distutils.util import strtobool
from fabric.api import *
from fabric.context_managers import settings
from fabric.colors import red, green, blue, cyan, magenta, white, yellow
from fabric.api import task
from fabric.state import env
from fabric.operations import local as lrun, run
from fabric.contrib.files import exists

import sys
import time
import datetime
import fabric.api as fab
import boto3
import requests
import json
import os

# vars
mysql_read_user = 'MYSQL_USER'
mysql_passwd = 'MYSQL_PASSWD'
mysql_host = 'MYSQL_HOST'
mysql_db_name = 'DB_NAME'
export_directory = 'db_export'
lock_file = 'db_export.lock'

# timestamp
stamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M')

# ignored tables (log tables)
log_tables_list = [
    'large_table',
    'another_large_table'
]

# env
env.keepalive = 1


def prep_db_dump():
    """
    Main call for getting the latest live database dump
    """
    # check if lock exists - if it does, exit.
    print 'Check if we can run..'
    if can_run():

        print 'We have a GO!'
        with cd('/tmp'):
            # create lock file
            print 'Creating lock file..'
            run('touch {lock_file}'.format(lock_file=lock_file))
            print 'Remove export directory'
            run('rm -rf {export_directory}'.format(export_directory=export_directory))

            # create export directory
            print 'Create export directory'
            run('mkdir {export_directory}'.format(export_directory=export_directory))

            with cd('{export_directory}'.format(export_directory=export_directory)):
                # do sql dumps
                print 'Dumping DB Schema'
                db_dump_schema()
                print 'Dumping log tables data'
                db_dump_log_data()
                print 'Dumping DB without log tables'
                db_dump_data_without_log_data()

            # remove lock file
            print 'Removing lock file..'
            run('rm -r {lock_file}'.format(lock_file=lock_file))


def db_dump_schema():
    """
    Dump database schema
    """
    run('time mysqldump -u{mysql_read_user} -p{mysql_passwd} -h{mysql_host} --add-drop-table --single-transaction --skip-lock-tables --no-data {mysql_db_name} | gzip -9 > {mysql_db_name}_schema.sql.gz'.format(
        mysql_read_user=mysql_read_user,
        mysql_passwd=mysql_passwd,
        mysql_host=mysql_host,
        mysql_db_name=mysql_db_name,
    ))


def db_dump_data_without_log_data():
    """
    Dump database without logs
    """
    run('time mysqldump -u{mysql_read_user} -p{mysql_passwd} -h{mysql_host} {ignored_tables} --add-drop-table --single-transaction --skip-lock-tables {mysql_db_name} | gzip -9 > {mysql_db_name}_data_without_log_tables.sql.gz'.format(
            mysql_read_user=mysql_read_user,
            mysql_passwd=mysql_passwd,
            mysql_host=mysql_host,
            mysql_db_name=mysql_db_name,
            ignored_tables=' '.join(map(lambda m: '--ignore-table=={mysql_db_name}.'.format(mysql_db_name=mysql_db_name)+m, log_tables_list))
        ))


def db_dump_log_data():
    """
    Dump log tables only
    """
    run('time mysqldump -u{mysql_read_user} -p{mysql_passwd} -h{mysql_host} {mysql_db_name} {log_tables} --add-drop-table --single-transaction --skip-lock-tables | gzip -9 > {mysql_db_name}_log_tables_data.sql.gz'.format(
            mysql_read_user=mysql_read_user,
            mysql_passwd=mysql_passwd,
            mysql_host=mysql_host,
            mysql_db_name=mysql_db_name,
            log_tables=' '.join(map(lambda m: ' ' + m, log_tables_list))
        ))


def can_run():
    """
    Check if lock file exists
    """
    if exists("/tmp/{lock_file}".format(lock_file=lock_file), use_sudo=False):
        print 'Lock file exists. Refusing to continue'
        sys.exit()
    else:
        return True
