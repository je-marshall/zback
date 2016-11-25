import threading
import multiprocessing
import Queue
import socket
import pickle
import ConfigParser
import subprocess
import datetime
import logging
import re
import os
import dataset

def run_command(command):
    ''' 
        Takes a command as a string and splits it, then runs it and returns
    '''
    log = logging.getLogger('zback.utils')
    if type(command) is not str:
        log.error("Cannot run command with args:")
        log.error(command)
        return False
    # Convention is to pass all commands as strings, but
    # subprocess.check_output wants them as lists
    # I like them as strings because they're easier to write
    run_cmd = subprocess.Popen(command.split(), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = run_cmd.communicate()
    if stderr:
        log.debug("Command failed: {0}".format(command))
        log.debug(stderr)
        raise subprocess.CalledProcessError
    elif stdout:
        return stdout
    else:
        return



def date_from_name(name):
    '''
        Takes a snapshot name and converts it to a datetime object
    '''
    log = logging.getLogger('zback.utils')
    fmt_string = '%Y-%m-%d-%H%M%S'
    try:
        date = datetime.datetime.strptime(name, fmt_string)
    except ValueError as e:
        log.debug("Timestamp format incorrect: {0}".format(name))
        log.debug(e)
        raise
    except Exception as e:
        log.debug("Error parsing timestamp: {0}".format(name))
        log.debug(e)
        raise

    return date

def name_from_date(date):
    '''
        Takes a datetime object and converts it to a snapshot name
    '''
    log = logging.getLogger('zback.utils')
    fmt_string = '%Y-%m-%d-%H%M%S'
    try:
        name = date.strftime(fmt_string)
    except AttributeError as e:
        log.warning("Incorrect value passed, not a date object")
        log.debug(e)
        return False

    return name

def date_from_string(date_string):
    '''
        Takes a ZFS string date output and converts it to a datetime object
    '''

    log = logging.getLogger('zback.utils')
    fmt_string = '%a %b %d %H:%M %Y'
    try:
        date = datetime.datetime.strptime(date_string, fmt_string)
    except ValueError as e:
        log.debug("Timestamp format incorrect: {0}".format(string))
        log.debug(e)
        raise

    return date

def sched_from_schema(schema):
    '''
        Takes a schema and turns it into a dict
    '''
    log = logging.getLogger('zback.utils')

    # So this also turns out to be much easier than I thought

    pattern = '[0-9]+[h|d|w|m]'

    schedule = re.findall(pattern, schema)

    if not schedule:
        log.warning("Invalid schedule: {0}".format(schema))
        raise ValueError

    hours = 0
    days = 0
    weeks = 0
    months = 0

    for entry in schedule:
        letter = filter(None, re.split('[0-9]+', entry))[0]
        number = filter(None, re.split('[a-z]', entry))[0]
        if letter == 'h':
            hours = int(number)
        elif letter == 'd':
            days = int(number)
        elif letter == 'w':
            weeks = int(number)
        elif letter == 'm':
            months = int(number)

    return {'hours' : hours, 'days' : days, 'weeks' : weeks, 'months' : months}

def schema_from_sched(sched):
    '''
        Takes a sched (dict) and parses it into a schema
    '''

    log = logging.getLogger('zback.utils')

    try:
        schema = "{0}h{1}d{2}w{3}m".format(sched['hours'],
                                           sched['days'],
                                           sched['weeks'],
                                           sched['months']
                                          )
        return schema
    except:
        log.debug("Invalid schedule supplied: {0}".format(sched))
        return False



def parse_destinations(destinations):
    '''
        Takes a destination string and parses it out into destinations and
        sending schedules
    '''

    log = logging.getLogger('zback.utils')

    return_list = []
    valid_send = ['hourly', 'daily', 'weekly', 'monthly']

    dest_list = destinations.split(",")

    for destination in dest_list:
        this_dest = destination.split("|")
        if len(this_dest) == 1:
            log.warning("Invalid destination specification {0} - must specify send schedule as well as dest".format(this_dest))
            continue
        if len(this_dest) == 2:
            if this_dest[1] not in valid_send:
                log.warning("Invalid send schedule {0}".format(this_dest[1]))
                continue
        return_list.append(this_dest)

    return return_list

def format_destinations(destinations):
    '''
        Takes a list of desintations and turns it into a string
    '''

    log = logging.getLogger('zback.utils')

    return_string = ''

    for dest in destinations:
        this_dest = "|".join(dest)
        return_string += this_dest
        return_string += ","

    return return_string.rstrip(",")


def parse_config(config_file, defaults):
    '''
        Parses a zback config file and returns a dictionary
    '''
    pass    
    if not os.path.isfile(config_file):
        return False

    config = ConfigParser.RawConfigParser()
    config.read(config_file)

    config_return = {}

    try:
        config_return['pidfile_path'] = config.get('zback', 'PID') 
    except:
        config_return['pidfile_path'] = defaults['pidfile_path']

    try:
        config_return['log_file'] = config.get('zback', 'LOG')  
    except:
        config_return['log_file'] = defaults['log_file']
    
    try:
        config_return['run_dir'] = config.get('zback', 'RUN')
    except:
        config_return['run_dir'] = defaults['run_dir']
    
    try:
        config_return['log_lvl'] = config.get('zback', 'LVL')
    except:
        config_return['log_lvl'] = defaults['log_lvl']
    
    try:
        config_return['limit'] = config.get('rate', 'limit')
    except:
        config_return['limit'] = defaults['limit']

    try:
        config_return['zdir'] = config.get('zback', 'zdir')
    except: 
        config_return['zdir'] = defaults['zdir']

    return config_return

def read_status(config):
    '''
        Opens a connection to the status output socket and parses what is in
        there via pickle to return a list of objects
    '''

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    address = config['client_socket']

    try:
        sock.connect(address)
    except socket.error as e:
        return False

    send_d = 'status'

    sock.send(send_d)

    data = ""
    part = None

    while part != "":
        part = sock.recv(4096)
        data += part

    try:
        format_data = pickle.loads(data)
        return format_data
    except pickle.UnpicklingError as e:
        return False

def get_open_port():
    '''
        Returns a random local port
    '''

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port

def snapshot_worker(snapshot_q):
    '''
    Gets snapshot properties
    '''

    log = logging.getLogger('witback.utils')

    while not snapshot_q.empty():
        this_snap = snapshot_q.get()

        try:
            this_snap.get_properties()
        except ValueError:
            log.error("Error parsing date for snapshot {0}".format(this_snap.name))
            snapshot_q.task_done()
            continue
        finally:
            snapshot_q.task_done()

def dataset_worker(dataset_q, snapshot_q):
    '''
    Gets dataset properties, then puts all snapshots for each set in 
    the snapshot queue, to be processed separately
    '''

    log = logging.getLogger('witback.utils')

    while not dataset_q.empty():
        this_set = dataset_q.get()
        try:
            this_set.get_properties()
        except ValueError:
            log.error("Invalid retention schema for dataset {0}".format(this_set.name))
            continue
        except AttributeError:
            log.debug("No snapshots for dataset {0}".format(this_set.name))
            continue
        finally:
            dataset_q.task_done()

        if this_set.snaplist is not None:
            for snap in this_set.snaplist:
                snapshot_q.put(snap)

def refresh_properties():
    '''
    Refreshes dataset and snapshot properties using threading for speed
    '''

    # This is fucking annoying - strptime not imported in a thread 
    # safe manner, see http://bugs.python.org/issue7980
    try:
        date_from_string("Stupid python")
    except:
        pass

    log = logging.getLogger('witback.utils')
    dataset_q = Queue.Queue()
    snapshot_q = Queue.Queue()

    max_threads = multiprocessing.cpu_count()

    try:
        set_list = dataset.Dataset.get_datasets()
    except subprocess.CalledProcessError as e:
        log.error(e)
        raise

    for this_set in set_list:
        dataset_q.put(this_set)

    if dataset_q.empty():
        raise Queue.Empty("No datasets found")
    
    for i in range(max_threads):
        worker = threading.Thread(target=dataset_worker, args=(dataset_q, snapshot_q))
        worker.setDaemon = True
        worker.start()
    
    dataset_q.join()

    if snapshot_q.empty():
        log.warning("No snapshots found")
        return set_list

    for i in range(max_threads):
        worker = threading.Thread(target=snapshot_worker, args=(snapshot_q,))
        worker.setDaemon = True
        worker.start()

    snapshot_q.join()

    return set_list