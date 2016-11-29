import sys
import os
import json
import argparse
import logging
import dataset
import utils


DESC = "SOME TEXT"

def main():
    '''
        Outputs formatted JSON for autodiscovery rules to be fed into zabbix
    '''
    
    parser = argparse.ArgumentParser(description=DESC)

    parser.add_argument('action', choices=['snap', 'prune', 'send'])

    args = parser.parse_args()

    # This bit just uses the default config for run dir. Would be nice to be
    # able to get it to respect whatever config is being used.
    jobs = utils.read_status(DEFAULT_CFG)

    log = logging.getLogger('zback')
    hndl = logging.NullHandler()
    log.addHandler(hndl)

    if not jobs:
        print "Error connecting to zback socket, check it is running"
        sys.exit(1)

    if args.action == 'snap':
        snap([ job for job in jobs if job['name'] == 'backup_job'])
    elif args.action == 'prune':
        prune([ job for job in jobs if job['name'] == 'prune_job'])
    elif args.action == 'send':
        send([ job for job in jobs if job['name'] == 'send_job'])

def snap(snap_list):
    '''
        Outputs datasets that have snapshots configured
    '''
    format_list = []

    for job in snap_list:
        for arg in job['args']:
            if isinstance(arg, dataset.Dataset):
                format_list.append({'{#DSNAME}' : arg.name})

    format_json(format_list)

def prune(prune_list):
    '''
        Outputs datasets that have prune tasks configured
    '''
    format_list = []

    for job in prune_list:
        for arg in job['args']:
            if isinstance(arg, dataset.Dataset):
                format_list.append({'{#DSNAME}' : arg.name})

    format_json(format_list)


def send(send_list):
    '''
        Outputs send locations for datasets
    '''
    pass

    format_list = []

    for job in send_list:
        this_job = []
        for arg in job['args']:
            if isinstance(arg, dataset.Dataset):
                this_job.append({'{#DSNAME}' : arg.name})
            else:
                this_job.append({'{#DSLOC}' : arg.location})
        
        if this_job:
            format_list.append(this_job)

    format_json(format_list)

def format_json(in_list):
    '''
        Formats the data gathered in a way that is parseable by zabbix
    '''
    json_string = {'data' : in_list}
    print json.dumps(json_string)

if __name__ == '__main__':
    main()
