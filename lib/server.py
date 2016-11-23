import logging
import os
import socket
import pickle
import threading
import subprocess
import dataset
import utils


class Server(object):
    '''
    Server process. Can run in onsite or offsite mode
    '''

    def __init__(self, config):
        self.running = False
        self.config = config
        self.current_tasks = []
        self.log = logging.getLogger('witback.server')
        self.q = Queue.Queue

    def while_thread(self, pipe, recv, task):
        '''
        Takes the mbuffer and zfs commands and loops on them until
        they complete. Don't care about handling its failure modes
        as this should simply be logged
        '''

        while recv.returncode is None:
            recv.poll()
            task['progress'] = pipe.stderr.readline()
        if recv.returncode == 0:
            self.log.info("Successfully received snapshot for dataset {0}".format(task['dataset']))
            self.current_tasks.remove(task)
        else:
            self.log.error("Error receiving snapshot for dataset {0}".format(task['dataset']))
            self.log.debug(recv.returncode)
            self.current_tasks.remove(task)
    
    def receive_handler(self, port, dataset):
        '''
        Called when an incoming send request is detected and starts an
        mbuffer process on the designated port, then pipes the result into
        a zfs receive command
        '''

        task = {'dataset' : dataset.name, 'port' : port, 'progress' : ''}
        self.current_tasks.append(task)

        pipe_cmd = 'mbuffer -L localhost:{0}'.format(port)
        recv_cmd = 'zfs recv -F {0}'.format(dataset.name)

        try:
            pipe = subprocess.Popen(pipe_cmd.split(), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            recv = subprocess.Popen(recv_cmd.split(), stdin=recv.stdout)

        except subprocess.CalledProcessError as e:
            self.log.error("Error receiving snapshot for dataset {0}".format(dataset.name))
            self.log.debug(e)
            raise RuntimeError("Mbuffer command failed")
            self.current_tasks.remove(task)

        loop = threading.Thread(target=self.while_thread, args=(pipe, recv, task))
        loop.daemon = True
        loop.start()
        self.log.info("Receive beginning for dataset {0}".format(dataset.name))

    def start(self):
        '''
        Binds to a local unix socket for communication and then loops
        '''

        datasets = dataset.Dataset.get_datasets()

        sock = socket.socket(socket.AF_UNIX)
        try:
            os.unlink(self.config['server_socket'])
        except OSError as e:
            self.log.error("Could not bind to local socket, server in use?")
            self.log.debug(e)

        sock.bind(self.config['server_socket'])
        sock.listen(5)

        while self.running:
            client, clientaddr = sock.accept()
            try:
                data = client.recv(4096)
                fmt_data = data.rstrip()

                if len(fmt_data) > 1:
                    self.log.debug("Unrecognised command")
                    continue
                
                this_dataset = [ds for ds in datasets if ds.name == fmt_data]
                if this_dataset:
                    this_port = utils.get_open_port()
                    try:
                        self.receive_handler(this_port, this_dataset)
                    except RuntimeError as e:
                        continue

                if fmt_data == 'status':
                    client.sendall("{0}\n".format(pickle.dumps(self.current_tasks, -1)))
            except:
                continue 
            finally:
                client.close()
