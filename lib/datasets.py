from operator import itemgetter
import subprocess
import datetime
import logging
import threading
import utils

PREFIX = None

class Dataset:
    '''
    Dataset properties and operations
    '''

    def __init__(self, name):
        self.log = logging.getLogger('witback.dataset')

        self.name = name
        self.snapshot = None
        self.snaplist = None
        self.retention = None
        self.destinations = None

        self.generic_get = 'zfs get -H -o value {0}:{1} {2}'

    def __getstate__(self):
        # Remove logger so can be pickled
        d = dict(self.__dict__)
        del d['log']
        return d

    def get_snapshot(self):

        command = self.generic_get.format(PREFIX, 'backup', self.name)
        
        try:
            snapshot = utils.run_command(command)
        except subprocess.CalledProcessError as e:
            self.log.error(e)
            raise

        if backup.rstrip() != '-':
            if backup.rstrip() == 'yes':
                self.snapshot = True
            elif backup.rstrip() == 'no':
                self.snapshot = False
    
    def get_retention(self):

        command = self.generic_get.format(PREFIX, 'retention', self.name)
        
        try:
            retention = utils.run_command(command)
        except subprocess.CalledProcessError as e:
            self.log.error(e)
            raise
        
        if retention.rstrip() != '-':
            try:
                schema = utils.sched_from_schema(retention.rstrip())
                self.retention = schema
            except ValueError as e:
                self.log.error(e)
                raise

    def get_destinations(self):

        command = self.generic_get.format(PREFIX, 'destinations', self.name)

        try:
            destinations = utils.run_command(command)
        except subprocess.CalledProcessError as e:
            self.log.error(e)
            raise

        if destinations.rstrip() != '-':
            dst_list = utils.parse_destinations(destinations.rstrip())

            if dst_list:
                self.destinations = dst_list
            else:
                self.log.debug("No destinations set for dataset {0}".format(self.name))
        else:
            self.log.debug("No desintations set for dataset {0}")

    def get_snaplist(self):

        command = 'zfs list -H -t snap -r {0} -o name'.format(self.name)

        try:
            unfmt_snaplist = utils.run_command(command)
        except subprocess.CalledProcessError as e:
            self.log.error(e)
        
        self.snaplist = []

        for snap in unfmt_snaplist.split():
            