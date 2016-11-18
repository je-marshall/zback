from operator import itemgetter
import parmiko
import subprocess
import logging
import dataset
import paramiko
import utils

def prune(dataset):
    '''
    Deletes all unneeded snapshots
    '''

    log = logging.getLogger('witback.prune')
    log.debug("Beginning prune on dataset {0}".format(dataset.name))

    # Need to refresh properties to get updated snaplist
    try:
        dataset.get_properties()
    except ValueError:
        log.error("Could not refresh properties for dataset {0}".format(dataset.name))
        raise RuntimeError("Refresh properties fail")

    if dataset.snaplist is None:
        log.error("No snapshots for dataset {0}, abandoning prune job".format(dataset.name))
        raise RuntimeError("Missing snapshot list")
    elif dataset.retention is None:
        log.error("No retention for dataset {0}, abdandoning prune job".format(dataset.name))
        raise RuntimeError("Missing retention") 


    snaps_to_delete = []

    months = []
    weeks = []
    days = []
    hours = []


    # So it turns out to be quite simple - build a list of hours, days, weeks
    # and months until each list gets full, which is dictated by the retention.
    # Once a list is full, any snapshot that meets the criteria gets put in the
    # delete bin instead.

    # NOTE - this bit will need to be revised now that snaps are not dicts
    for snap in sorted(dataset.snaplist, key=lambda x: x.date, reverse=True):
        if snap.date.hour == 00:
            if len(hours) < dataset.retention['hours']:
                hours.append(snap)
            if len(days) < dataset.retention['days']:
                days.append(snap)
            if snap.date.weekday() == 6 and len(weeks) < dataset.retention['weeks']:
                weeks.append(snap)
            if snap.date.day == 1 and len(months) < dataset.retention['months']:
                months.append(snap)
        elif len(hours) < dataset.retention['hours']:
            hours.append(snap)
        if snap not in hours and snap not in days and snap not in weeks and snap not in months:
            snaps_to_delete.append(snap)

    for snap in snaps_to_delete:
        try:
            snap.get_properties()
        except ValueError:
            log.error("Error getting properties for snapshot {0}".format(snap.name))
            raise RuntimeError("Snapshot properties fail")
        if snap.holds is None:
            try:
                snap.destroy()
            except:
                log.info("Skipping snapshot {0}, enable debug log for more info".format(snap.name))
                continue
        else:
            continue

    log.info("Completed prune job for dataset {0}".format(dataset.name))

def snapshot(dataset):
    '''
    Creates a snapshot of a dataset
    '''
    log = logging.getLogger('witback.snapshot')
    log.info("Beginning snapshot run on dataset {0}".format(dataset.name))

    try:
        dataset.take_snapshot()
    except:
        log.error("Error taking snapshot for dataset {0}, enable debug log for more info".format(dataset.name))
        raise RuntimeError("Snapshot fail")

    log.info("Snapshot run completed successfully for dataset {0}".format(dataset.name))

def send(dataset, location, ssh_config_file=None):
    '''
    Sends a snapshot to a location
    '''
    # Try to ssh into location, bail if any errors
    # Check if remote server is running
    # Ask for latest snapshot on remote end
    # Request ssh forward from other side
    # Begin sending into local end of port forward
    # Confirm snapshot received on remote end 

    log = logging.getLogger('witback.send')

    try:
        dataset.get_properties()
    except ValueError:
        log.error("Could not refresh dataset properties for dataset {0}".format(dataset.name))
        raise RuntimeError("Error getting dataset properties")

    ssh = paramiko.SSHClient()
    ssh.load_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh_config = paramiko.SSHConfig()

    if ssh_config_file is None:
        with open('~/.ssh/config') as f:
          ssh_config.parse(f)  
    else:
        ssh_config.parse(ssh_config_file)
    
    host_lookup = ssh_config.lookup(location.split(':')[0])
    if len(host_lookup) == 1:
        log.error("Host {0} not configured in ssh config file".format(location))
        raise RuntimeError("Error with ssh config")
    
    try:
        ssh.connect(host_lookup['hostname'], username=host_lookup['user'], port=host_lookup['port'])
    except Exception as e:
        log.error("Could not connect to remote host {0}".format(location))
        log.debug(e)
        raise

    check_command = 'zfs list -H -t snap -r {0} -o name'.format(location.split(':')[1])

    stdin, stdout, stderr = ssh.exec_command(check_command)
    if not stderr.read():
        latest_remote = stdout.read().split().pop()
    else:
        log.error("Error checking for remote snapshot")
        log.debug(stderr.read())
        raise RuntimeError("Could not check remote snapshot")
    
    # NOTE - need to figure out how to request the remote end to start an mbuffer
    # And get a random port...
    # That should go here ^^

    transport = ssh.get_transport()
    try:
        channel = transport.open_channel("direct-tcpip", dest_addr=('127.0.0.1', port), src_addr=('', 0))
    except paramiko.SSHException as e:
        log.error("Could not open forwarding channel")
        log.debug(e)
        raise RuntimeError("Channel creation failed")
    
    try:
        dataset.send(latest_remote, channel)
    except RuntimeError:
        raise
