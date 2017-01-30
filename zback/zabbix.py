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

    # This bit needs thinking about

    # format_list = []

    # jobs = utils.temp_send_message_client(socket)
    # send_list = [ job for job in jobs if job['name'] == 'send']

    # for job in send_list:
    #     this_job = []
    #     for arg in job['args']:
    #         if isinstance(arg, dataset.Dataset):
    #             this_job.append({'{#DSNAME}' : arg.name})
    #         elif isinstance(arg, sender.Location):
    #             this_job.append({'{#DSLOC}' : arg.location})
        
    #     if this_job:
    #         format_list.append(this_job)

    # format_json(format_list)

def format_json(in_list):
    '''
        Formats the data gathered in a way that is parseable by zabbix
    '''
    json_string = {'data' : in_list}
    print json.dumps(json_string)