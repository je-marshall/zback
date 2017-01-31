import sys
import os
import json
import argparse
import logging
from zback import utils, dataset

def snap(socket):
    '''
        Outputs datasets that have snapshots configured
    '''
    format_list = []

    jobs = utils.temp_send_message_client(socket, 'status')
    snap_list = [ job for job in jobs if job['name'] == 'snapshot']

    for job in snap_list:
        for arg in job['args']:
            if isinstance(arg, dataset.Dataset):
                format_list.append({'{#DSNAME}' : arg.name})

    format_json(format_list)

def prune(socket):
    '''
        Outputs datasets that have prune tasks configured
    '''
    format_list = []

    jobs = utils.temp_send_message_client(socket, 'status')
    prune_list = [ job for job in jobs if job['name'] == 'prune']

    for job in prune_list:
        for arg in job['args']:
            if isinstance(arg, dataset.Dataset):
                format_list.append({'{#DSNAME}' : arg.name})

    format_json(format_list)


def send(socket):
    '''
        Outputs send locations for datasets
    '''
    pass

    # Quick and dirty like your mum

    format_list = [] 
    jobs = utils.temp_send_message_client(socket, 'status')
    send_list = [ job for job in jobs if job['name'] == 'send']

    for job in send_list:
        this_job = []
        this_job.append({'{#DSNAME}' : job['args'][0].name})
        this_job.append({'{#DSLOC}' : job['args'][1]})

        if this_job:
            format_json(this_job)
        
    #     if this_job:
    #         format_list.append(this_job)

    # format_json(format_list)

def format_json(in_list):
    '''
        Formats the data gathered in a way that is parseable by zabbix
    '''
    json_string = {'data' : in_list}
    print json.dumps(json_string)