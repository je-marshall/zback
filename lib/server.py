import logging
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

    def add_jobs(self):
        '''
        Adds jobs to the scheduler - only prune tasks necessary
        '''

        try:
            setlist = utils.refresh_properties()
        except subprocess.CalledProcessError:
            self.log.error("Could not get dataset list, check debug log")
            raise RuntimeError("Could not get datasets")
        except Queue.Empty:
            self.log.error("No datasets found")
            raise RuntimeError("Could not get datasets")

        for this_set in setlist:
            if this_set.retention:
                self.scheduler.add_job(jobs.prune,
                                       'cron',
                                       second=30,
                                       minute=0,
                                       args=[this_set]
                                      )
                self.log.info("Added prune job for dataset {0}".format(this_set.name))

    def 