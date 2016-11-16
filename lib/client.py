import logging
import Queue
import subprocess
import jobs
import dataset
import utils
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

class Client(object):
    '''
    Handles scheduling of snapshot and prune tasks as well as sending
    '''

    def __init__(self, config):
        self.config = config
        self.scheduler = BackgroundScheduler()
        self.log = logging.getLogger('witback.client')
    
    def add_jobs(self):

        try:
            setlist = utils.refresh_properties()
        except subprocess.CalledProcessError:
            self.log.error("Could not get dataset list, check debug log")
            raise RuntimeError("Could not get datasets")
        except Queue.Empty:
            self.log.error("No datasets found")
            raise RuntimeError("Could not get datasets")

        for this_set in setlist:
            # Set up prune job if there is a retention specified
            if this_set.retention is not None:
                self.scheduler.add_job(jobs.prune, 'cron', second=30, minute=0, args=[this_set])
                self.log.info("Added hourly pruning job for dataset {0}".format(this_set.name))

            # Set up snapshot tasks if snapshotting is specified AND if there is a retention
            if this_set.retention is not None:
                if this_set.snapshot:
                    if this_set.retention['hours'] > 0:
                        self.scheduler.add_job(jobs.snapshot, 'cron', second=0, minute=0,
                                               args=[this_set])

                        self.log.info("Added hourly snapshot task for dataset {0}".format(
                            this_set.name))

                    elif this_set.retention['days'] > 0:
                        self.scheduler.add_job(jobs.snapshot, 'cron', second=0, minute=0,
                                               hour=0, args=[this_set])

                        self.log.info("Added daily snapshot task for dataset {0}".format(
                            this_set.name))

                    elif this_set.retention['weeks'] > 0:
                        self.scheduler.add_job(jobs.snapshot, 'cron', second=0, minute=0,
                                               hour=0, day='sun', args=[this_set])

                        self.log.info("Added weekly snapshot task for dataset {0}".format(
                            this_set.name))

                    elif this_set.retention['months'] > 0:
                        self.scheduler.add_job(jobs.snapshot, 'cron', second=0, minute=0,
                                               hour=0, day=0, args=[this_set])

                        self.log.info("Added monthly snapshot task for dataset {0}".format(
                            this_set.name))
                else:
                    self.log.info("Not backing up dataset {0}".format(this_set.name))
            else:
                self.log.info("Incorrect or missing retention for dataset {0}".format(
                    this_set.name))


            if this_set.destinations is not None:
                for location in this_set.destinations:
                    if location[1] == 'hourly':
                        self.scheduler.add_job(jobs.send, 'cron', second=0, minute=0,
                                               args=[this_set, location[0]])

                        self.log.info("Added hourly send for dataset {0} to location {1}".format(
                            this_set.name, location[0]))

                    elif location[1] == 'daily':
                        self.scheduler.add_job(jobs.send, 'cron', second=0, minute=0,
                                               hour=0, args=[this_set, location[0]])

                        self.log.info("Added daily send for dataset {0} to location {1}".format(
                            this_set.name, location[0]))

                    elif location[1] == 'weekly':
                        self.scheduler.add_job(jobs.send, 'cron', second=0, minute=0,
                                               hour=0, day='sun', args=[this_set, location[0]])

                        self.log.info("Added weekly send for dataset {0} to location {1}".format(
                            this_set.name, location[0]))

                    elif location[1] == 'monthly':
                        self.scheduler.add_job(jobs.send, 'cron', second=0, minute=0,
                                               hour=0, day=0, args=[this_set, location[0]])

                        self.log.info("Added monthly send for dataset {0} to location {1}".format(
                            this_set.name, location[0]))
                    else:
                        self.log.warning("Dataset {0} has incorrectly formatted destination {1}".format(
                            this_set.name, location))

            else:
                self.log.info("No destinations set for dataset {0}".format(this_set.name))

        def external_monitor(self, event):
            '''
            Runs an external monitor program. This can be modified so long as it 
            can handle the output of this function, documented in the wiki
            '''

            this_job = [job for job in self.scheduler.get_jobs() if job.id == event.job_id][0]

            try:
                if this_job.name == 'send':
                    dataset = this_job.args[0].name
                    send_loc = this_job.args[1]
                else:
                    dataset = this_job.args[0].name
                    send_loc = ""

                run_cmd = "{0}/bin/external-monitor {1} {2} {3} {4}".format(
                    self.config['root_dir'],
                    event.code,
                    this_job.name,
                    dataset,
                    send_loc
                )

                utils.run_command(run_cmd)
            
            except subprocess.CalledProcessError:
                self.log.error("Error running monitor command: {0}".format(run_cmd))
            except Exception as e:
                self.log.error(e)

            if event.exception:
                self.log.debug("Event exception: {0}".format(event.exception))
            if event.traceback:
                self.log.debug("Event traceback: {0}".format(event.traceback))

