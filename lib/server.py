import logging
import paramiko
import Queue
import subprocess
import dataset
import utils
import jobs
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

class Server(object):
    '''
    Server process. Can run in onsite or offsite mode
    '''

    def __init__(self, config):
        self.config = config
        self.scheduler = BackgroundScheduler()
        self.log = logging.getLogger('witback.server')
    
    def receive_handler(self, port, dataset):
        '''
        Called when an incoming send request is detected and starts an 
        mbuffer process on the designated port, then pipes the result into
        a zfs receive command
        '''

        zfs_cmd = 'zfs recv -F {0}'.format(dataset.name)
        mbuf_cmd = 'mbuffer -L localhost:{0}'.format(port)

        try:
            mbuf = subprocess.Popen(mbuf_cmd.split(), stdout=subprocess.PIPE)
            zfs = subprocess.Popen(zfs_cmd.split(), stdin=mbuf.stdout)

            while zfs.returncode is None:
                zfs.poll()
            if zfs.returncode == 0:
                self.log.info("Successfully recevied snapshot for dataset {0}".format(dataset.name))
            else:
                self.log.error("Error receiving snapshot for dataset {0}".format(dataset.name))
                raise RuntimeError("ZFS recv returned non-zero")
        except subprocess.CalledProcessError as e:
            self.log.error("Error receiving snapshot for dataset {0}".format(dataset.name))
            self.log.debug(e)
            raise RuntimeError("Mbuffer command failed")
            


