import socket
import threading
import Queue
import SocketServer
import pickle
import time
import sys
import os
import subprocess
import logging
import dataset
import utils
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
        # def handle(self):
        # data = self.request.recv(1024)
        # cur_thread = threading.current_thread()
        # response = "{}: {}".format(cur_thread.name, data)
        # self.request.sendall(response)

    def handle(self):
        data = self.request.recv(4096)
        
        # Ideally this would be a pickle, for more advanced requests
        fmt_data = str(data.rstrip())

        self.server.log.info("Incoming request")

        if fmt_data:
            try:
                this_dataset = [ds for ds in self.server.datasets if ds.name == fmt_data][0]
                if this_dataset:
                    this_port = utils.get_open_port()
                    self.receive(this_port, this_dataset)
            except IndexError:
                self.server.log.debug("Non dataset request")
            if fmt_data == 'shutdown':
                self.server.log.info("Received shutdown command")
                self.server.server_close()

    def receive(self, port, dataset):

        self.log = logging.getLogger("zback.server")

        self.log.info("Receiving stream for dataset {0}".format(dataset.name))

        pipe_cmd = 'mbuffer -I 127.0.0.1:{0}'.format(port)
        recv_cmd = 'zfs recv -F {0}'.format(dataset.name)

        try:
            pipe = subprocess.Popen(pipe_cmd.split(), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            recv = subprocess.Popen(recv_cmd.split(), stdin=pipe.stdout)

            self.log.debug("Started pipe process {0} with pid {1}".format(pipe_cmd, pipe.pid))
            self.log.debug("Started receive process {0} with pid {1}".format(recv_cmd, recv.pid))

        except subprocess.CalledProcessError as e:
            self.server.log.error("Error starting receive processes for dataset {0}".format(dataset.name))
            self.server.log.debug(e)
            try:
                pipe.kill()
                recv.kill()
            except:
                pass
            raise

        self.request.sendall(str(port))

        while recv.returncode is None:
            recv.poll()
        if recv.returncode == 0:
            self.server.log.info("Successfully received snapshot for dataset{0}".format(dataset.name))
        else:
            self.server.log.error("Error receiving snapshot for dataset {0}".format(dataset.name))
            self.server.log.debug(recv.returncode)


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

class ZbackServer(object):
    '''
    Main server class, does prep and handles reloads
    '''

    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger('zback.server')
        self.scheduler = BackgroundScheduler()
        self.server = None

    def refresh_setlist(self):
        '''
        Refreshes the setlist and pushes it to the SocketServer 
        '''
        try:
            setlist = utils.refresh_properties()
        except subprocess.CalledProcessError:
            self.log.error("Error querying datasets")
            sys.exit(1)
        except Queue.Empty:
            self.log.error("Could not find any datasets, check ZFS installed and sets configured")
            sys.exit(1)
        
        self.server.datasets = setlist


    def start(self):
        '''
        Starts the server, unless it is already running
        '''
        srv_host = self.config['server_addr']
        srv_port = self.config['server_port']

        # Set up the scheduler to periocially query the current dataset list
        # This generates a lot of annoying logs, need to revise
        # self.scheduler.add_job(self.refresh_setlist, 'cron', second=0)
        # self.scheduler.start()

        self.server = ThreadedTCPServer((srv_host, srv_port), ThreadedTCPRequestHandler)
        self.refresh_setlist()
        self.server.log = self.log

        server_thread = threading.Thread(target=self.server.serve_forever)
        server_thread.daemon = True
        server_thread.start()

        while server_thread.isAlive:
            time.sleep(60)

        self.log.info("Shutting down")
        sys.exit(1)
