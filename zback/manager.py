import argparse
import socket
import pickle
import sys
import utils
import dataset
import server
import client

'''
This is pretty inelegant as it stands but it ought to work
'''

DESC = ''

def main():
    '''
    Simple management wrapper around client.py and server.py
    '''

    config_dict = read_config()
    parser = argparse.ArgumentParser(description=DESC)

    parser.add_argument('component', choices=['client', 'server'])
    parser.add_argument('action', choices=['start',
                                           'stop',
                                           'restart',
                                           'status',
                                           'configure'])
    args = parser.parse_args()

    if args.component == 'client':
        client_handler(config_dict, args.action)
    elif args.component == 'server':
        server_handler(config_dict, args.action)


    

    pass

def read_config(config_file):
    '''
    Parses out various config values
    '''

    pass

def server_handler(config_dict, action):
    '''
    Handles server based operations
    '''

    if action == 'start':
        this_server = server.ZbackServer(config_dict)
        print "Starting server"
        this_server.start()
    elif action == 'stop':
        print "Sending shutdown command to server"
        try:
            utils.send_message(config_dict['server_adress'],
                               config_dict['server_port'],
                               'stop')
        except socket.error:
            print "Error connecting to server, is server running?"
            sys.exit(1)
        except pickle.UnpicklingError:
            sys.exit(0)

        sys.exit(0)

    elif action == 'restart':
        sock = socket.socket(socket.AF_UNIX)
        try:
            sock.connect(config_dict['server_socket'])
        except socket.error:
            print "Could not connect to server socket, is server running?"
            sys.exit(1)
        print "Sending shutdown command to server"
        sock.send(pickle.dumps("stop"))
        this_server = server.Server(config_dict)
        print "Starting server"
        this_server.start()
    elif action == 'status':
        stats = utils.read_status(config_dict['server_socket'])
        # This needs refining
        print stats
        sys.exit(0)
    else:
        print "Unknown command {0}".format(action)
        sys.exit(1)


def client_handler(config_dict, action):
    '''
    Handles client based operations
    '''

    if action == 'start':
        this_client = client.Client(config_dict)
        print "Starting client"
        this_client.start()
    elif action == 'stop':
        sock = socket.socket(socket.AF_UNIX)
        try:
            sock.connect(config_dict['client_socket'])
        except socket.error:
            print "Could not connect to client socket, is client running?"
            sys.exit(1)
        print "Sending shutdown command to client"
        sock.send(pickle.dumps("stop"))
        sys.exit(0)
    elif action == 'restart':
        sock = socket.socket(socket.AF_UNIX)
        try:
            sock.connect(config_dict['client_socket'])
        except socket.error:
            print "Could not connect to client socket, is client running?"
            sys.exit(1)
        print "Sending shutdown command to client"
        sock.send(pickle.dumps("stop"))
        this_server = client.Client(config_dict)
        print "Starting client"
        this_server.start()
    elif action == 'status':
        stats = utils.read_status(config_dict['client_socket'])
        # This needs refining
        print stats
        sys.exit(0)
    elif action == 'configure':
        # This needs to be implemented, possibly with a separate module
        print "Not implemented yet"
    else:
        print "Unknown command {0}".format(action)
        sys.exit(1)
