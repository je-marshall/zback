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
    
    def receive_handler(self, port, dataset):
        '''
        Called when an incoming send request is detected and starts an
        mbuffer process on the designated port, then pipes the result into
        a zfs receive command
        '''
        task = {'dataset' : dataset.name, 'port' : port, 'progress' : ''}
        self.current_tasks.append(task)

        pipe_cmd = 'mbuffer -I localhost:{0}'.format(port)
        recv_cmd = 'zfs recv -F {0}'.format(dataset.name)

        try:
            pipe = subprocess.Popen(pipe_cmd.split(), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            recv = subprocess.Popen(recv_cmd.split(), stdin=pipe.stdout)

        except subprocess.CalledProcessError as e:
            self.log.error("Error receiving snapshot for dataset {0}".format(dataset.name))
            self.log.debug(e)
            self.current_tasks.remove(task)
            try:
                pipe.kill()
                recv.kill()
            except:
                pass
            raise RuntimeError("Mbuffer command failed")

        while recv.returncode is None:
            recv.poll()
            task['progress'] = pipe.stderr.readline()
        if recv.returncode == 0:
            self.log.info("Successfully received snapshot for dataset {0}".format(dataset.name))
            self.current_tasks.remove(task)
        else:
            self.log.error("Error receiving snapshot for dataset {0}".format(dataset.name))
            self.log.debug(recv.returncode)
            self.current_tasks.remove(task)
        self.log.info("Receive beginning for dataset {0}".format(dataset.name))

    def stop(self):
        self.running = False

    def start(self):
        '''
        Binds to a local unix socket for communication and then loops
        '''

        datasets = dataset.Dataset.get_datasets()

        sock = socket.socket(socket.AF_UNIX)
        # try:
        #     os.unlink(self.config['server_socket'])
        # except OSError as e:
        #     self.log.error("Could not bind to local socket, server in use?")
        #     self.log.debug(e)

        sock.bind(self.config['server_socket'])
        sock.listen(5)

        self.running = True

        while self.running:
            client, clientaddr = sock.accept()
            try:
                data = client.recv(4096)
                self.log.debug(data)
                fmt_data = str(data.rstrip())

                this_dataset = [ds for ds in datasets if ds.name == fmt_data][0]
                if this_dataset:
                    this_port = utils.get_open_port()
                    try:
                        self.log.debug("Starting receive process")
                        client.sendall(str(this_port))
                        loop = threading.Thread(target=self.receive_handler, args=(this_port, this_dataset))
                        loop.daemon = True
                        loop.start()
                    except RuntimeError as e:
                        self.log.error("Error starting receive process")
                        self.log.debug(e)
                        continue
                    except Exception as e:
                        self.log.debug(e)

                if fmt_data == 'status':
                    client.sendall("{0}\n".format(pickle.dumps(self.current_tasks, -1)))
            except:
                continue 
            finally:
                client.close()
