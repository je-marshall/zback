import socket
import threading
import SocketServer
import pickle
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
        datasets = dataset.Dataset.get_datasets()
        log = logging.getLogger('zback.server')
        data = self.request.recv(4096)
        fmt_data = str(data.rstrip())

        log.info("Incoming request")

        this_dataset = [ds for ds in datasets if ds.name == fmt_data][0]
        if this_dataset:
            this_port = utils.get_open_port()
            self.receive(this_port, this_dataset)

    def receive(self, port, dataset):

        log = logging.getLogger("zback.server")

        pipe_cmd = 'mbuffer -I 127.0.0.1:{0}'.format(port)
        recv_cmd = 'zfs recv -F {0}'.format(dataset.name)

        try:
            pipe = subprocess.Popen(pipe_cmd.split(), stdout = subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            recv = subprocess.Popen(recv_cmd.split(), stdin=pipe.stdout)

        except subprocess.CalledProcessError as e:
            log.error("Error starting receive process for dataset {0}".format(dataset.name))
            log.debug(e)
            try:
                pipe.kill()
                recv.kill()
            except:
                pass
            raise
        
        self.request.sendall(str(port)

        while recv.returncode is None:
            recv.poll()
        if recv.returncode == 0:
            log.info("Successfully received snapshot for dataset{0}".format(dataset.name))
        else:
            log.error("Error receiving snapshot for dataset {0}".format(dataset.name))
            log.debug(recv.returncode)


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

    # def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
    #     self.log = logging.getLogger('zback.server')
    #     self.datasets = dataset.Dataset.get_datasets()
    #     SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=True)


if __name__ == '__main__':

    HOST, PORT = "127.0.0.1", 5230
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    server_thread = threading.Thread(target = server.serve_forever)
    server_thread.daemon = True
    server_thread.start()