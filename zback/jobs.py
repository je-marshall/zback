from operator import itemgetter
import socket
import pickle
import paramiko
import subprocess
import threading
import logging
import time
import paramiko
import utils
import dataset
import forward

def prune(this_set):
    '''
    Deletes all unneeded snapshots
    '''

    log = logging.getLogger('zback.prune')
    log.debug("Beginning prune on dataset {0}".format(this_set.name))

    # Need to refresh properties to get updated snaplist
    try:
        this_set.get_properties()
        for snap in this_set.snaplist:
            snap.get_properties()
    except ValueError:
        log.error("Could not refresh properties for dataset {0}".format(this_set.name))
        raise RuntimeError("Refresh properties fail")

    if this_set.snaplist is None:
        log.error("No snapshots for dataset {0}, abandoning prune job".format(this_set.name))
        raise RuntimeError("Missing snapshot list")
    elif this_set.retention is None:
        log.error("No retention for dataset {0}, abdandoning prune job".format(this_set.name))
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
    this_set.snaplist.sort(key=lambda item:item.date, reverse=True)
    for snap in this_set.snaplist:
        if snap.date.hour == 00:
            if len(hours) < this_set.retention['hours']:
                hours.append(snap)
            if len(days) < this_set.retention['days']:
                days.append(snap)
            if snap.date.weekday() == 6 and len(weeks) < this_set.retention['weeks']:
                weeks.append(snap)
            if snap.date.day == 1 and len(months) < this_set.retention['months']:
                months.append(snap)
        elif len(hours) < this_set.retention['hours']:
            hours.append(snap)
        if snap not in hours and snap not in days and snap not in weeks and snap not in months:
            snaps_to_delete.append(snap)

    if not snaps_to_delete:
        log.info("No snapshots to delete for dataset {0}".format(this_set.name))

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

    log.info("Completed prune job for dataset {0}".format(this_set.name))

def snapshot(this_set):
    '''
    Creates a snapshot of a dataset
    '''
    log = logging.getLogger('zback.snapshot')
    log.info("Beginning snapshot run on dataset {0}".format(this_set.name))

    try:
        this_set.take_snapshot()
    except:
        log.error("Error taking snapshot for dataset {0}, enable debug log for more info".format(this_set.name))
        raise RuntimeError("Snapshot fail")

    log.info("Snapshot run completed successfully for dataset {0}".format(this_set.name))

def send(this_set, location, config):
    '''
    Sends a snapshot to a location
    '''
    # Try to ssh into location, bail if any errors
    # Check if remote server is running
    # Ask for latest snapshot on remote end
    # Request ssh forward from other side
    # Begin sending into local end of port forward
    # Confirm snapshot received on remote end 

    log = logging.getLogger('zback.send')

    # Make sure the properties for the dataset are up to date and bail if
    # they can't be accessed
    try:
        this_set.get_properties()
        for snap in this_set.snaplist:
            snap.get_properties()
    except ValueError:
        log.error("Could not refresh dataset properties for dataset {0}".format(this_set.name))
        raise RuntimeError("Error getting dataset properties")

    if not this_set.snaplist:
        log.error("No snapshots for local dataset {0}, aborting".format(this_set.name))
        raise RuntimeError("No snapshots")

    # Initialise the ssh client and set its defaults
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh_config = paramiko.SSHConfig()

    # Use the config supplied ssh config file, otherwise revert to sys default
    if not config['general']['ssh_config_file']:
        with open('~/.ssh/config') as f:
          ssh_config.parse(f)
    else:
        with open(config['general']['ssh_config_file']) as f:
            ssh_config.parse(f)

    # Attempt a host lookup from the config file
    host_lookup = ssh_config.lookup(location.split(':')[0])
    if len(host_lookup) == 1:
        log.error("Host {0} not configured in ssh config file".format(location))
        raise RuntimeError("Error with ssh config")
    
    # Attempt to connect to the looked up host
    log.debug("Attempting to connect to host {0}".format(location))
    try:
        ssh.connect(host_lookup['hostname'], username=host_lookup['user'], port=int(host_lookup['port']))
    except Exception as e:
        log.error("Could not connect to remote host {0}".format(location))
        log.debug(e)
        raise
    
    # Check what the latest snapshot is on the remote end
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
            ssh.close()
            raise RuntimeError("No remote snapshots")
    else:
        log.error("Error checking for remote snapshot")
        log.debug(err)
        ssh.close()
        raise RuntimeError("Could not check remote snapshot")

    # Format the snapshot so we can check for it on the local side
    latest_remote_fmt = "{0}@{1}".format(this_set.name, latest_remote.split("@")[1])

    present = [snap for snap in this_set.snaplist if snap.name == latest_remote_fmt]
    if not present:
        log.error("Latest remote snapshot {0} is not present on local side, need to reseed".format(latest_remote_fmt))
        raise RuntimeError("Remote snapshot not present locally")
    
    # Get the latest local snapshot so as to be able to send an incremental
    this_set.snaplist.sort(key=lambda item: item.date)
    latest_local = this_set.snaplist.pop()
    log.debug("Sending snapshot: {0}".format(latest_local.name))

    if latest_local.name == latest_remote_fmt:
        log.info("Remote snapshot up to date for dataset {0}".format(this_set.name))
        ssh.close()
        return

    # Get the underlying ssh transport to request some channels
    transport = ssh.get_transport()

    # First channel connects to the server process on the other end and asks it to set up 
    # an mbuffer and zfs recv process, which, if successful, will return a port number

    # So this is quite complicated - might be easier to replace this with a separate binary
    # that can be called, which only returns when the mbuffer proc is fully up on the other end
    try:
        req_chan = transport.open_channel("direct-tcpip",
                                          dest_addr=('127.0.0.1',
                                                     int(config['client']['server_port'])),
                                          src_addr=('', 0))
        log.debug("Opened channel to remote host {0}, requesting receive process".format(location))
        req_pickle = pickle.dumps(location.split(':')[1])
        req_chan.sendall(req_pickle)
        # Timeout
        req_chan.settimeout(30.0)
        try:
            data = False
            while not data:
                data = req_chan.recv(4096)
        except socket.timeout:
            log.error("Waited too long for server to respond to request for dataset {0}, exiting".format(this_set.name))
            raise RuntimeError("Timed out waiting for server")
        try:
            r_port = pickle.loads(data.rstrip())
            if r_port == 'ERROR':
                log.debug("Error creating mbuffer process on server, check logs")
                raise RuntimeError("Could not transfer snapshot, check server logs")
        except Exception as e:
            log.error("Error getting port for remote receive process, aborting")
            log.debug("Received: {0}".format(data))
            log.debug(e)
            raise RuntimeError("Remote receive failed")
    except paramiko.SSHException as e:
        log.error("Failed to open forwarding channel to remote host {0}".format(location))
        log.debug(e)
        ssh.close()
        raise RuntimeError("Channel open failed")

    l_port = utils.get_open_port([])

    try:
        forward_handler = forward.make_forward_handler(('127.0.0.1', r_port), transport, log)
        forward_server = forward.TCPServer(('127.0.0.1', l_port), forward_handler)
        server_thread = threading.Thread(target=forward_server.handle_request)
        log.debug("Opened forward tunnel to remote host {0}".format(location))
        server_thread.start()
        time.sleep(1)
    except Exception as e:
        log.debug(e)
        ssh.close()
        raise RuntimeError("Failed to open forwarding channel")

    # # Second channel is the one we will be forwarding the zfs send across
    # try:
    #     forward_chan = transport.open_channel("direct-tcpip", dest_addr=('127.0.0.1', port), src_addr=('', 0))
    #     log.debug("Opened channel to remote host {0}".format(location))
    # except paramiko.SSHException as e:
    #     log.error("Failed to open forwarding channel to remote host {0}".format(location))
    #     log.debug(e)
    #     ssh.close()
    #     raise RuntimeError("Channel open failed")

    log.debug("Starting send process")
    print latest_remote_fmt
    print latest_local.name

    try:
        send_cmd = 'zfs send -i {0} {1}'.format(latest_remote_fmt, latest_local.name)
        buff_cmd = 'nc 127.0.0.1 {0}'.format(l_port)
        # buff_cmd = 'mbuffer -l /tmp/zback-client-{0}.log -O 127.0.0.1:{0}'.format(l_port)

        log.debug(send_cmd)
        log.debug(buff_cmd)

        send = subprocess.Popen(send_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        buff = subprocess.Popen(buff_cmd.split(), stdin=send.stdout)

        while send.returncode is None:
            send.poll()
            log.debug(send.stderr.readline())

        while buff.returncode is None:
            buff.poll()

        if send.returncode == 0 and buff.returncode == 0:
            try:
                confirm_data = pickle.loads(req_chan.recv(4096))
                if confirm_data == 'SUCCESS':
                    ssh.close()
                    log.info("Send successful for dataset {0}".format(this_set.name))
                else:
                    ssh.close()
                    log.error("Remote receive failed for dataset {0} to destination {1}".format(this_set.name, location))
                    raise RuntimeError("Failed to send snapshot")
            except socket.timeout:
                log.debug("Failed to receive response from destination {0} for dataset {1}".format(location, this_set.name))
                raise RuntimeError("Cannot confirm send successful")
        else:
            log.debug("Sending snapshot {0} failed with returncode {1}".format(
                latest_local.name, send.returncode))
            ssh.close()
            raise RuntimeError("Send failed")
    except subprocess.CalledProcessError as e:
            log.debug(e)
            raise RuntimeError("Command failed")

    # # Forward the data across the channel until the local send has finished
    # while send.returncode is None:
    #     send.poll()
    #     for line in send.stdout:
    #         forward_chan.send(line)
    
    # # Check the returncode to see if anything went awry
    # if send.returncode == 0:
    #     forward_chan.close()
    # else:
    #     log.debug("Sending snapshot {0} failed with returncode {1}".format(
    #         latest_local.name, send.returncode))
    #     ssh.close()
    #     raise RuntimeError("Send failed")


    # If snapshot sent successfully, put a hold on it

    ref = location.split(':')[0]
    try:
        latest_local.hold(ref)
    except TypeError:
        log.warning("Could not hold snapshot {0}".format(latest_local.name))
    except subprocess.CalledProcessError:
        log.warning("Could not hold snapshot {0}".format(latest_local.name))

    # Remove any other holds with this ref
    held_snaps = [snap for snap in this_set.snaplist if snap.holds is not None]
    for snap in held_snaps:
        if ref in snap.holds:
            try:
                snap.unhold(ref)
            except:
                log.warning("Could not unhold snapshot {0}".format(snap.name))
    ssh.close()
