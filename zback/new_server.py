import socket
import threading
import SocketServer
import pickle
import sys
import os
import subprocess
import logging
import dataset
import utils

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
                self.server.shutdown()

    def receive(self, port, dataset):

        self.log = logging.getLogger("zback.server")

        pipe_cmd = 'mbuffer -I 127.0.0.1:{0}'.format(port)
        recv_cmd = 'zfs recv -F {0}'.format(dataset.name)

        try:
            pipe = subprocess.Popen(pipe_cmd.split(), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            recv = subprocess.Popen(recv_cmd.split(), stdin=pipe.stdout)

        except subprocess.CalledProcessError as e:
            self.server.log.error("Error starting receive process for dataset {0}".format(dataset.name))
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

    # def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
    #     self.log = logging.getLogger('zback.server')
    #     self.datasets = dataset.Dataset.get_datasets()
    #     SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=True)

class ZbackServer(object):
    '''
    Main server class, does prep and handles reloads
    '''

    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger('zback.server')

    def start(self):
        '''
        Starts the server, unless it is already running
        '''
        srv_host = self.config['server_addr']
        srv_port = self.config['server_port']

        try:
            datasets = dataset.Dataset.get_datasets()
        except subprocess.CalledProcessError:
            self.log.error("ZFS not installed, exiting")
            sys.exit(1)
        except RuntimeError:
            self.log.error("No ZFS datasets found, exiting")
            sys.exit(1)

        server = ThreadedTCPServer((srv_host, srv_port), ThreadedTCPRequestHandler)
        server.datasets = datasets
        server.log = self.log

        # server_thread = threading.Thread(target=server.serve_forever)
        # server_thread.daemon = True
        # server_thread.start()

        server.serve_forever()
