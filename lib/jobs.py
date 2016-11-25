from operator import itemgetter
import paramiko
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
        for snap in dataset.snaplist:
            snap.get_properties()
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
    dataset.snaplist.sort(key=lambda item:item.date)
    for snap in dataset.snaplist:
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

    if not snaps_to_delete:
        log.info("No snapshots to delete for dataset {0}".format(dataset.name))

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

def send(dataset, location, config):
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
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh_config = paramiko.SSHConfig()

    if not config['ssh_config_file']:
        with open('~/.ssh/config') as f:
          ssh_config.parse(f)
    else:
        with open(config['ssh_config_file']) as f:
            ssh_config.parse(f)
    
    host_lookup = ssh_config.lookup(location.split(':')[0])
    if len(host_lookup) == 1:
        log.error("Host {0} not configured in ssh config file".format(location))
        raise RuntimeError("Error with ssh config")

    log.debug("Attempting to connect to host {0}".format(location))
    try:
        ssh.connect(host_lookup['hostname'], username=host_lookup['user'], port=int(host_lookup['port']))
    except Exception as e:
        log.error("Could not connect to remote host {0}".format(location))
        log.debug(e)
        raise

    check_command = 'zfs list -H -t snap -r {0} -o name'.format(location.split(':')[1])

    log.debug("Checking for latest remote snapshot on host {0}".format(location))
    c_stdin, c_stdout, c_stderr = ssh.exec_command(check_command)
    err = c_stderr.read()
    if not err:
        try:
            latest_remote = c_stdout.read().split().pop()
            log.debug("Latest snapshot {0} found on host {1}".format(latest_remote, location))
        except IndexError:
            log.error("No remote snapshots in location {0}".format(location))
    else:
        log.error("Error checking for remote snapshot")
        log.debug(err)
        raise RuntimeError("Could not check remote snapshot")

    port_command = 'echo {0} | nc -U {1}'.format(location.split(':')[1], config['server_socket'])

    p_stdin, p_stdout, p_stderr = ssh.exec_command(port_command)
    try:
        port = int(p_stdout.read())
        log.debug("Received port {0} from remote host {1}".format(port, location))
    except ValueError as e:
        log.error("Could not get remote port from host {0}".format(location.split(':')[0]))
        log.debug(e)
        raise

    transport = ssh.get_transport()
    try:
        channel = transport.open_channel("direct-tcpip", dest_addr=('127.0.0.1', port), src_addr=('', 0))
        log.debug("Opened channel to remote host {0}".format(location))
    except paramiko.SSHException as e:
        log.error("Could not open forwarding channel")
        log.debug(e)
        raise RuntimeError("Channel creation failed")

    latest_remote_fmt = "{0}@{1}".format(dataset.name, latest_remote.split("@")[1])
    
    try:
        log.debug("Beginning send for remote host {0}".format(location))
        dataset.send(latest_remote_fmt, channel)
    except RuntimeError:
        raise
    finally:
        ssh.close()
