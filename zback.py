#!/usr/bin/env python3
from logging.handlers import WatchedFileHandler
from datetime import datetime
from threading import Thread
import subprocess
import argparse
import logging
import random
import socket
import json
import time
import sys
import os
import re
import paramiko

# Globals, because I'm not messing around with loading config files. If you want
# to change these go ahead.
# PROP must be of the form [a-z]*: otherwise it makes it a pain to send to
# Zabbix, feel free to ignore this stipulation if you have turned off sending.
PROP = 'zback:'
TIMEFMT = '%Y-%m-%d-%H%M'
DEFTAGS = {'hourly' : '48', \
         'daily' : '7', \
         'weekly' : '6', \
         'monthly' : '6'}
ZBX_SEND = True
ZBX_CONF = '/etc/zabbix/zabbix_agentd.conf'
ZBX_BIN = '/bin/zabbix_sender'
LOG_DIR = '/var/log/zback/'
LOG_LVL = logging.DEBUG
PROTOCOLS = ('tcp', 'ssh')

class Dataset():
    '''
    Base Dataset class, implements local dataset handling functions. Is intended
    to be subclassed to provide remote functionality.
    '''
    def __init__(self, name, logger):
        self.name = name
        self.props = None
        self.log = logger
        self.tags = None
        self.snaplist = None
        self.holdlist = None
        self.latest_snap = None
    def _exec_command(self, command):
        '''
        Executes a command on a local dataset only. This function should be
        overriden for remote datasets. If the command is successful it returns
        the output, otherwise it logs and raises
        '''
        try:
            result = subprocess.check_output(command.split(), \
                                             stderr=subprocess.PIPE, \
                                             universal_newlines=True)
            self.log.debug('successful command: %s' % command)
        except subprocess.CalledProcessError:
            self.log.debug('failed command: %s' % command)
            self.log.error('Error running ZFS command')
            raise
        # TODO Figure out better subprocess.stderr handling
        return result
    def _clear_holds(self):
        '''
        V Dangerous, only really for testing
        '''
        self.get_holdlist()
        for held_snap in self.holdlist:
            holds = self.get_holds(held_snap)
            for hold in holds:
                try:
                    self.release_snap(held_snap, hold)
                except subprocess.CalledProcessError as exception:
                    self.log.error('Failed to clear holds on snap %s', held_snap)
                    self.log.debug(exception)
                    continue
    def _get_tags(self):
        '''
        Only intended to be called when getting properties. If tags are defined
        and make sense use em, if not stick with the defaults
        '''
        try:
            self.tags = {k:v for k,v in [tag.split(':') for tag in \
                                   self.props['tags'].split(',')]}
        except KeyError:
            self.log.debug('No tags specified, using defaults')
            self.tags = DEFTAGS
    def get_props(self):
        '''
        Updates the internal property dictionary
        '''
        prop_cmd = 'zfs get all -H -o property,value %s' % self.name
        all_props = self._exec_command(prop_cmd)
        self.props = {re.sub(PROP, '', k):v for k, v in \
                      [prop.split('\t') for prop in all_props.split('\n') \
                      if PROP in prop]}
        self._get_tags()
    def set_prop(self, prop, value):
        '''
        Set a property
        '''
        set_cmd = 'zfs set %s=%s %s' % (PROP+prop, value, self.name)
        self._exec_command(set_cmd)
    def clear_prop(self, prop):
        '''
        Clears a property
        '''
        clear_cmd = 'zfs inherit %s %s' % (PROP+prop, self.name)
        self._exec_command(clear_cmd)
    def get_holdlist(self):
        '''
        Update internal holdlist
        '''
        holdlist_cmd = 'zfs get userrefs -H -r %s' % self.name
        holdlist = self._exec_command(holdlist_cmd)
        self.holdlist = [snap.split('\t')[0] for snap in holdlist.split('\n') \
                   if '@' in snap and int(snap.split('\t')[-2])]
    def get_holds(self, snap):
        '''
        Returns a formatted list of holds placed on a snapshot
        '''
        holds_cmd = 'zfs holds -H %s' % snap
        holds = self._exec_command(holds_cmd)
        return [hold.split('\t')[1].strip() for hold in \
                holds.strip('\n').split('\n')]
    def get_snaps(self):
        '''
        Updates internal snaplist
        '''
        snap_cmd = 'zfs list -H -t snap -s creation -o name -r %s' % self.name
        all_snaps = self._exec_command(snap_cmd)
        self.snaplist = [snap for snap in all_snaps.rstrip('\n').split('\n') \
                   if PROP in snap]
    def take_snap(self, tag):
        '''
        Takes a snapshot
        '''
        nowstamp = PROP + datetime.now().strftime(TIMEFMT) + ':%s' % tag
        take_cmd = 'zfs snap {0}@{1}'.format(self.name, nowstamp)
        self._exec_command(take_cmd)
        # Set the latest snapshot now, for sending purposes
        self.get_snaps()
        self.latest_snap = self.snaplist[-1]
    def destroy_snap(self, snap):
        '''
        Destroys a snapshot
        '''
        destroy_cmd = 'zfs destroy %s' % snap
        self._exec_command(destroy_cmd)
    def hold_snap(self, snap, ref):
        '''
        Places a userref on a snapshot
        '''
        hold_cmd = 'zfs hold %s %s' % (ref, snap)
        self._exec_command(hold_cmd)
    def release_snap(self, snap, ref):
        '''
        Releases a hold placed on a snapshot
        '''
        release_cmd = 'zfs release %s %s' % (ref, snap)
        self._exec_command(release_cmd)
    def send(self, increm=None):
        '''
        Something only a source dataset can do
        '''
        self.get_snaps()
        if increm:
            send_cmd = ('zfs send -c -i %s %s' % (increm, \
                        self.latest_snap)).split()
        else:
            send_cmd = ('zfs send -c %s' % self.latest_snap).split()
        try:
            sendproc = subprocess.Popen(send_cmd, stdout=subprocess.PIPE)
            self.log.debug('successful cmd: %s' % send_cmd)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.error('Error running ZFS send command')
            raise
        # TODO Figure out better subprocess.stderr handling
        return sendproc
    def receive(self, sendproc, initialise=False):
        '''
        Receive for local dataset only, should be subclassed for remote
        '''
        self.log.info('Sending to %s...', self.name)
        if initialise:
            recv_cmd = 'zfs recv -F %s' % self.name
        else:
            recv_cmd = 'zfs recv %s' % self.name
        recvproc = subprocess.Popen(recv_cmd.split(), \
                                    stdin=sendproc.stdout)
        while recvproc.poll() is None:
            time.sleep(1)
        self.log.info('Send successful')

class RemoteDataset(Dataset):
    '''
    Subclassed to override a couple of functions. On initialisation attempts to
    establish an SSH connection to the remote side and will raise if not
    '''
    def __init__(self, name, host, protocol, logger):
        super().__init__(name, logger)
        self.name = name
        self.host = host
        self.protocol = protocol
        self.sshclient = None
        self._get_ssh_client()
        # Run this now to throw an instant error
        self.get_snaps()
    def _get_ssh_client(self):
        try:
            self.sshclient = paramiko.client.SSHClient()
            self.sshclient.load_system_host_keys()
            self.sshclient.connect(self.host, banner_timeout=120, auth_timeout=120)
        except paramiko.ssh_exception.BadHostKeyException as exception:
            self.log.exception(exception)
            raise ConnectionError('Could not connect to host %s, ' \
                                  'bad host key', self.host)
        except paramiko.ssh_exception.AuthenticationException as exception:
            self.log.exception(exception)
            raise ConnectionError('Could not connect to host %s, ' \
                                  'authentication failure', self.host)
        except paramiko.ssh_exception.SSHException as exception:
            self.log.exception(exception)
            raise ConnectionError('Could not connect to host %s, ' \
                                  'check logs', self.host)
        except socket.error as exception:
            self.log.exception(exception)
            raise ConnectionError('Socket error while connecting to ' \
                                  '%s', self.host)
        except IndexError as exception:
            self.log.exception(exception)
            raise ConnectionError('Destination incorrectly formatted for ' \
                                  'dataset %s', self.name)
        except IOError as exception:
            self.log.exception(exception)
            raise ConnectionError('Could not load system host keys')
        except Exception as exception:
            self.log.exception(exception)
            raise ConnectionError('Unexpection! Check debug logs for exception info')
    def _exec_command(self, command):
        '''
        Runs a remote command and outputs in the same way as the super class
        does, returns the stdout if possible and raises if not
        '''
        try:
            cmd_in, cmd_out, cmd_err = self.sshclient.exec_command(command)
            cmd_out_fmt = cmd_out.read().decode()
            cmd_err_fmt = cmd_err.read().decode()
            if cmd_err_fmt:
                self.log.debug('stderr: %s' % cmd_err_fmt)
                self.log.warning('Remote command returned stderr')
                self.log.warning('remote command: %s', command)
        except paramiko.ssh_exception.SSHException as exception:
            self.log.debug(exception)
            self.log.error('Remote command failed')
            self.log.error('remote command: %s', command)
            raise
        self.log.debug('successful cmd: %s', command)
        return cmd_out_fmt
    def _nc_listen(self, recv_cmd):
        '''
        Attempt to start a netcat listen process on the remote end. This is
        useful for either a direct send or a send via SSH. If sending via SSH
        establish a listener only on localhost
        '''
        # Ephemeral ports, pretty unlikely to be used
        port = random.randint(32768, 60999)
        # Get IP address from the point of view of the remote server, so as to
        # whitelist it for netcat
        try:
            ipin, ipout, iperr = self.sshclient.exec_command('echo $SSH_CLIENT')
            ip = ipout.read().decode().split()[0]
        except paramiko.ssh_exception.SSHException as exception:
            self.log.debug(exception)
            self.log.error('Could not obtain IP address, netcat cannot start')
            raise ConnectionError
        if self.protocol == 'ssh':
            nc_cmd = 'nc localhost -l %i -e "%s"' % (port, recv_cmd)
        else:
            nc_cmd = 'nc -l %s -e "%s" --allow %s' % (port, recv_cmd, ip)
        self.log.debug(nc_cmd)
        try:
            # Set it up and send back the port that got used
            ncin, ncout, ncerr = self.sshclient.exec_command(nc_cmd, timeout=5)
            return (port, ncerr)
        except paramiko.ssh_exception.SSHException as exception:
            self.log.debug(exception)
            self.log.error('Netcat command could not start')
            raise ConnectionError
        except socket.timeout as exception:
            self.log.debug(exception)
            self.log.warning('Timed out waiting for nc command')
            raise ConnectionError
    def _get_connection(self, port):
        '''
        Wraps _nc_listen and returns either a direct socket connection or a
        paramiko Channel that can be written to and responds to the same calls
        to close
        '''
        try:
            if self.protocol == 'ssh':
                self.log.debug('Opening SSH channel')
                transport = self.sshclient.get_transport()
                channel = transport.open_channel('direct-tcpip', \
                                                 dest_addr=('localhost', port), \
                                                 src_addr=('localhost', port))
                self.log.debug('SSH channel opened')
                return channel
            if self.protocol == 'tcp':
                self.log.debug('Opening TCP socket')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.host, port))
                self.log.debug('Socket opened')
                return sock
        except socket.error as exception:
            self.log.error('Socket connection failed')
            self.log.debug(exception)
            raise ConnectionError
        except paramiko.ssh_exception.SSHException as exception:
            self.log.error('SSH channel open failed')
            self.log.debug(exception)
            raise ConnectionError
    def receive(self, sendproc, initialise=False):
        '''
        Takes a subprocess as input and reads from its stdout. If the initialise
        flag is set, it attempts a -F recv
        '''
        self.log.info('Sending to %s...', self.host)
        if initialise:
            recv_cmd = '/sbin/zfs recv -F %s' % self.name
        else:
            recv_cmd = '/sbin/zfs recv %s' % self.name
        self.log.debug('recv_cmd: %s' % recv_cmd)
        self.log.debug('Setting up NC listener on remote')
        port, nc_err = self._nc_listen(recv_cmd)
        self.log.debug('nc listen established on port %s', port)
        # Quick sleep to allow nc to get ready
        time.sleep(1)
        self.log.debug('Establishing connection to remote port %i', port)
        conduit = self._get_connection(port)
        while not nc_err.channel.exit_status_ready():
            data = sendproc.stdout.read(4096)
            try:
                if data:
                    conduit.send(data)
                else:
                    break
            except socket.timeout:
                if sendproc.poll() != 0:
                    self.log.debug('Error with local send process')
                    raise
                else:
                    break
        try:
            nc_error = nc_err.read()
            if nc_error:
                self.log.error('Netcat error: %s', nc_error)
                raise ConnectionError
        except socket.timeout:
            self.log.debug('No netcat error to read')
        self.log.info('Send successful')
        conduit.close()

class DatasetProcessor(Thread):
    '''
    Accepts a local dataset and does a run on it. This consists of taking a
    snapshot if appropriate, sending if appropriate and pruning if appropriate
    '''
    def __init__(self, source_dataset, args):
        Thread.__init__(self)
        self.source = source_dataset
        self.run_tag = args.tag
        self.initialise = args.init
        self.send_only = args.resend
        self.tags = None
        self.log = self.source.log
        self.destination = None
        self.sending_snap = None
    def _snapshot(self):
        '''
        Takes a snapshot if necessary
        '''
        # If the resend option has been set then skip snapshot
        if self.send_only:
            return
        # Check if this is a snapshot that is being sent from somewhere
        # else. If it is then there is no need to take a new snapshot
        try:
            if self.run_tag in self.source.props['recv']:
                self.log.info('Tag received from source, no need to snap')
                return
        except KeyError:
            self.log.debug('No recv tags specified')
        # Assuming no
        try:
            self.log.info('Taking snapshot...')
            self.source.take_snap(tag=self.run_tag)
            zbx_send(self.source.name, 'snap', '0', self.log)
        except subprocess.CalledProcessError:
            zbx_send(self.source.name, 'snap', '1', self.log)
            self.log.error('Snapshot already exists or zfs command issue')
            return 1
        self.sending_snap = self.source.latest_snap
    def _send(self):
        '''
        Performs checks to set up send jobs
        '''
        try:
            send_string = self.source.props['send'].split(',')
            send_jobs = [substr for substr in send_string \
                         if self.run_tag in substr]
        except KeyError:
            self.log.debug('No send jobs specified')
            return
        except Exception as exception:
            self.log.debug(exception)
            return
        if send_jobs:
            self.log.debug('send jobs: %s', send_jobs)
            self.source.get_holdlist()
            send_fail = False
            zbx_send(self.source.name, 'sending', '0', self.log)
            for job in send_jobs:
                try:
                    zbx_send(self.source.name, 'sending', '1', self.log)
                    self._send_receive(job)
                    zbx_send(self.source.name, 'sending', '0', self.log)
                except (BlockingIOError, \
                        LookupError, \
                        ConnectionError):
                    # Known error, logs should reflect this already
                    send_fail = True
                    continue
                except Exception as exception:
                    # All known exceptions are handled at lower levels with
                    # debug logging, this is to catch anything that hasn't
                    # already been anticipated...
                    send_fail = True
                    self.log.exception(exception)
                    self.log.error("Unexpected error during send job")
                    continue
            if send_fail:
                zbx_send(self.source.name, 'sent', '1', self.log)
            else:
                zbx_send(self.source.name, 'sent', '0', self.log)
        else:
            self.log.debug('No send jobs with this tag')
            return
    def _prune(self):
        '''
        Using this run_tag, figures out how many snapshots need to be deleted
        and then does the necessary tidying up
        '''
        # Check if the resend option was set
        if self.send_only:
            return
        # Otherwise, sort it out
        self.log.info('Beginning prune')
        self.source.get_snaps()
        try:
            relevant_snaps = [snap for snap in self.source.snaplist \
                              if PROP in snap and self.run_tag in snap]
        except IndexError:
            self.log.debug('No snapshots to prune')
            return
        del_list = []
        while len(relevant_snaps) > int(self.source.tags[self.run_tag]):
            # Reverse pop because we want oldest snap
            del_list.append(relevant_snaps.pop(0))
        for snap in del_list:
            try:
                self.source.destroy_snap(snap)
            except subprocess.CalledProcessError:
                continue
        self.log.info('Pruning complete')
    def _send_receive(self, job):
        '''
        Handles the full transfer of snapshots including holding snapshots
        '''
        dest_str = re.sub('%s:' % self.run_tag, '', job)
        self.log.debug('dest_str: %s', dest_str)
        send_hold = '%s:SEND' % dest_str
        self.log.debug('send_hold: %s', send_hold)
        try:
            for snap in self.source.holdlist:
                if send_hold in self.source.get_holds(snap):
                    # Abort if ANY send is in progress for this dataset. This is
                    # important as sends are meant to be called sequentially,
                    # not sure what the deal would be with sending the same
                    # snapshot to two places at the same time would be...
                    self.log.error('Send in progress to %s, aborting', \
                                   dest_str)
                    raise BlockingIOError('Send in progress')
        except IndexError:
            self.log.debug('No held snapshot')
        # Figure out what type of send we're doing and instantiate the right
        # class:
        if 'local:' in dest_str.split(':')[0]:
            self.log.debug('Destination is local')
            dest_name = dest_str.split(':')[-1], \
                self.destination = Dataset(dest_name, self.log)
        else:
            self.log.debug('Destination is remote')
            protocol, host, dest_name = dest_str.split(':')
            self.destination = RemoteDataset(dest_name, host, protocol, \
                                                self.log)
        self.log.debug('Destination set up, checking latest matching snap')
        # Now that we have established a connection the destination dataset we
        # can put a send hold on as in theory we are able to start a send
        self.log.debug('Placing send hold before starting send')
        self.source.hold_snap(self.sending_snap, send_hold)
        # Now that we have a destination dataset set up, we need to check their
        # respective snaplists:
        try:
            # At this point, check if the destination has any snapshots. If it
            # does, refuse to initialise it.
            if self.destination.snaplist:
                if self.initialise:
                    self.log.error('Will not initialise destination as it has snapshots')
                    self.source.release_snap(self.sending_snap, send_hold)
                    raise BlockingIOError
                latest_remote = [snap for snap in self.destination.snaplist \
                                if self.run_tag in snap].pop()
                src_match = [snap for snap in self.source.snaplist \
                            if snap.split('@')[-1] == latest_remote.split('@')[-1] \
                            and self.run_tag in snap].pop()
            else:
                src_match = None
        except IndexError:
            self.log.debug('No common snapshots')
            if not self.initialise:
                self.source.release_snap(self.sending_snap, send_hold)
                raise LookupError('No snapshots in common and not initialising')
            src_match = None
        self.log.debug('src_match: %s', src_match)
        try:
            self.log.debug('Beginning send')
            sendproc = self.source.send(src_match)
            self.destination.receive(sendproc, initialise=self.initialise)
        except ConnectionError:
            # Doesn't matter what went wrong, pull the send hold and bail
            self.log.debug('Error during sending, attempting to release send hold')
            self.source.release_snap(self.sending_snap, send_hold)
            raise
        # Have a little nap to allow the snapshots on the remote end to be updated
        time.sleep(5)
        # Destination hold reflects where it came from rather than its dest.
        # Also latest remote is subject to the run we're doing - we don't manage
        # the other end's snapshots we just be sendin
        self.destination.get_snaps()
        if self.destination.snaplist:
            latest_remote = [snap for snap in self.destination.snaplist \
                            if self.run_tag in snap].pop()
            self.destination.hold_snap(latest_remote, self.source.name)
        else:
            self.log.error('No remote snapshots present after send')
            self.log.error('Check logs and connection to destination')
            raise ConnectionError
        # Assuming that the send went correctly, then remove the send hold:
        self.source.release_snap(self.sending_snap, send_hold)
        # Place a successful sent hold on source and dest
        self.source.hold_snap(self.sending_snap, dest_str)
        # Check all other holds on snaps and remove any that are no longer
        # relevant
        self.source.get_holdlist()
        for snap in self.source.holdlist:
            if snap != self.sending_snap:
                try:
                    self.source.release_snap(snap, dest_str)
                except subprocess.CalledProcessError:
                    continue
        # Do the same for the remote end
        self.destination.get_holdlist()
        for snap in self.destination.holdlist:
            if snap != self.destination.latest_snap:
                try:
                    self.destination.release_snap(snap, self.source.name)
                except subprocess.CalledProcessError:
                    continue
        # If this was an init run, set dest props
        if self.initialise:
            self.destination.set_prop('manage', 'yes')
            self.destination.set_prop('recv', self.run_tag)
    def run(self):
        '''
        Performs a run on a dataset.
        '''
        self.log.info('Beginning run with tag %s', self.run_tag)
        self.source.get_props()
        self.log.debug(self.source.props)
        # If this run is not a specified tag then bail
        if self.run_tag not in self.source.tags.keys():
            self.log.info('Specified run tag does not match dataset tags or defaults')
            return
        # Check if the set is paused
        if 'paused' in self.source.props:
            if self.source.props['paused'] == 'yes':
                self.log.info('Datset paused, skipping')
                return
        # Run the individual steps
        self._snapshot()
        self._send()
        self._prune()
        self.log.info('zback run completed')

def zbx_send(dsname, key, value, log):
    '''
    Sends updates to Zabbix. Does not check validity of global config variables.
    Can also be disabled by setting ZBX_SEND to False. In either that case or a
    successful send it returns, if it fails it logs and returns anyway
    '''
    if not ZBX_SEND:
        return
    send_cmd = [ZBX_BIN, '-c', ZBX_CONF, \
                '-k', PROP.replace(':', '.')+key+'[%s]' % dsname, \
                '-o', value]
    try:
        zabbix_send = subprocess.check_output(send_cmd, universal_newlines=True)
    except subprocess.CalledProcessError:
        log.debug('send_cmd: '+' '.join(send_cmd))
        log.warning('Error sending to zabbix, check config')
        return

def get_zback_sets():
    '''
    Returns a list of zback managed datasets
    '''
    get_cmd = ['zfs', 'get', '-H', '-o', 'name,property,value', \
               '-d', '1', '-t', 'filesystem', '%smanage' % PROP]
    try:
        zback_sets = subprocess.check_output(get_cmd, universal_newlines=True)
    except subprocess.CalledProcessError:
        return None
    return [dset.split('\t')[0] for dset in zback_sets.strip('\n').split('\n') \
            if dset.split('\t')[-1] == 'yes' and '@' not in dset]

def add_streamhandler(logger):
    '''
    FileHandler is subclass of StreamHandler so this actually matches both
    classes. However if we always call this first, then it doesn't matter as the
    only type of logger added would be a stream one anyhow
    '''
    formatter = logging.Formatter('%(asctime)s::%(levelname)s: %(message)s', \
                                  '%H:%M:%S')
    if logger.handlers:
        if [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]:
            return
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return

def add_filehandler(logger, logfile):
    '''
    Sets up a file log for the logger, note different format
    '''
    formatter = logging.Formatter('%(asctime)s::%(levelname)s: %(message)s')
    if logger.handlers:
        if [h for h in logger.handlers if isinstance(h, logging.FileHandler)]:
            return
    logpath = LOG_DIR + logfile.split('/')[-1] + '.log'
    handler = WatchedFileHandler(logpath)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return

def query_yes_no(question, default='no'):
    valid = {'yes': True, 'y': True, 'ye': True, \
             'no': False, 'n': False}
    while True:
        choice = input(question).lower()
        if choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            print('Please respond y/n')

def process_cmd(args):
    '''
    Performs a run on datasets, spawns a new thread for each one
    '''
    if args.resend and args.init:
        print("Cannot resend and initialise at the same time")
        sys.exit(1)
    if args.dataset == 'all':
        if args.init:
            print("Refusing to initialise all datasets")
            sys.exit(1)
        if args.resend:
            print("Resend only available for individual datasets")
            sys.exit(1)
        for dataset in args.dsets:
            this_log = logging.getLogger('zback.%s' % dataset)
            if args.filelogging:
                add_filehandler(this_log, dataset)
            this_thread = DatasetProcessor(Dataset(dataset, this_log), args)
            this_thread.start()
    elif args.dataset in args.dsets:
        this_log = logging.getLogger('zback.%s' % args.dataset)
        if args.init:
            add_streamhandler(this_log)
        if args.filelogging:
            add_filehandler(this_log, args.dataset)
        this_run = DatasetProcessor(Dataset(args.dataset, this_log), args)
        this_run.start()
        this_run.join()
        sys.exit(0)
    else:
        print('Dataset not managed by zback, check and try again.')
        sys.exit(1)

def configure_cmd(args):
    '''
    Adds or edits zback related properties for a specified dataset
    '''
    this_set = args.dataset.pop()
    log = logging.getLogger('zback.%s' % this_set)
    if args.dataset in args.dsets:
        if not query_yes_no('This set already managed by zback, overwrite? [y/N]: '):
            return
    if not query_yes_no('Manage %s with zback? [y/N]: ' % this_set):
        return
    else:
        configure_set = Dataset(this_set, log)
        configure_set.set_prop('manage', 'yes')
    if not query_yes_no('Use default tags? [Y/n]: ', default='yes'):
        tag_list = []
        while True:
            tag_name = input('Name for the tag: ')
            # Could include some regex here at some point to check the tag
            tag_count = input('Number of snapshots with this tag that you want to keep: ')
            try:
                this_tag = '%s:%d' % (tag_name, int(tag_count))
                print('Adding tag: %s' % this_tag)
                tag_list.append(this_tag)
            except ValueError:
                print('Please try entering the number of snapshots again')
                continue
            if query_yes_no('Would you like to add more tags? [y/N]: '):
                continue
            else:
                break
        configure_set.set_prop('tags', ','.join(tag_list))
        configure_set.get_props()
    if query_yes_no('Add send job? [y/N]: '):
        send_list = []
        while True:
            send_tag = input('Tag to send: ')
            if configure_set.tags == None:
                set_tags = DEFTAGS
            else:
                set_tags = configure_set.tags
            if send_tag not in set_tags.keys():
                print('Tag not recognised, make sure it has been added')
                continue
            send_protocol = input('Protocol to send with : ')
            if send_protocol not in PROTOCOLS:
                print('Not a valid protocol. Currently supported are: %s' % \
                      (PROTOCOLS,))
                continue
            # No checking done on these because NO
            send_destination = input('Destination to send to: ')
            send_dataset = input('Destination dataset: ')
            this_send_job = ':'.join([send_tag, send_protocol, \
                                     send_destination, send_dataset])
            print('Adding send job: %s' % this_send_job)
            send_list.append(this_send_job)
            if query_yes_no('Add another send job? [y/N]: '):
                continue
            else:
                break
        configure_set.set_prop('send', ','.join(send_list))

def pause_cmd(args):
    '''
    Places a flag on a dataset or datasets that prevents further zback runs,
    useful for maintenance - doesn't remove any other configuration
    '''
    if args.dsets is None:
        print('No datasets managed by zback')
        sys.exit(1)
    if args.dataset:
        setlist = args.dataset
    else:
        setlist = args.dsets
    log = logging.getLogger('zback')
    for dset in setlist:
        this_set = Dataset(dset, log)
        if args.unpause:
            print('Unpausing %s' % dset)
            this_set.clear_prop('paused')
            continue
        else:
            print('Pausing %s' % dset)
            this_set.set_prop('paused', 'yes')
            continue

def clear_cmd(args):
    '''
    Clears zback related settings and snapshots from a dataset
    '''
    if args.dsets is None:
        print('No datasets managed by zback')
        sys.exit(1)
    # Thought about checking if this is a zback managed dset but tbh there isn't
    # a hell of a lot of point, either it is and clearing works or it isn't and
    # it fails...
    log = logging.getLogger('zback')
    clear_set = Dataset(args.dataset.pop(), log)
    clear_set.get_props()
    possible_props = ['send', 'recv', 'manage', 'tags']
    if args.props or args.full:
        for prop in possible_props:
            print('Clearing property: %s' % prop)
            clear_set.clear_prop(prop)
    if args.holds or args.full:
        print('Clearing send holds')
        clear_set._clear_holds()
    if args.snaps or args.full:
        print('Destroying all zback related snapshots')
        clear_set.get_snaps()
        for snap in clear_set.snaplist:
            clear_set.destroy_snap(snap)

def zabbix_cmd(args):
    '''
    Returns JSON formatted string of zback managed datasets for Zabbix
    '''
    if args.dsets is None:
        print('No datasets managed by zback')
        sys.exit(1)
    print(json.dumps({'data': [{"{#ZBACKDSET}": dset} for dset in args.dsets]}))
    sys.exit(0)

def help_cmd(args):
    print('Please specify a subcommand, or use --help to see options')

def main():
    '''
    Parse args and decide what to do
    '''
    # Set up logging
    log = logging.getLogger('zback')
    log.setLevel(LOG_LVL)
    filelogging = True
    if not os.path.exists(LOG_DIR):
        add_streamhandler(log)
        log.setLevel(logging.DEBUG)
        log.warning('No logdir configured, console logging enabled')
        filelogging = False
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands')
    parser.set_defaults(func=help_cmd)
    parser.add_argument('-v', '--verbose', action='store_true', \
        help='Logs to console and increases logging output')
    parser.add_argument('--autocomplete', action='store_true', \
        help=argparse.SUPPRESS)

    parser_process = subparsers.add_parser('process', \
        help=process_cmd.__doc__.strip())
    parser_process.set_defaults(func=process_cmd)
    parser_process.add_argument('dataset', nargs='?', default='all', \
        help='Defaults to all zback managed datasets')
    parser_process.add_argument('tag', \
        help='REQUIRED: tag for backup run')
    parser_process.add_argument('--init', action='store_true', \
        help='Initialise destination dataset if sending')
    parser_process.add_argument('--resend', action='store_true', \
        help='Do not snapshot or prune, just attempt to resend last snapshot NOT IMPLEMENTED YET')

    parser_configure = subparsers.add_parser('configure', \
        help=configure_cmd.__doc__.strip())
    parser_configure.set_defaults(func=configure_cmd)
    parser_configure.add_argument('dataset', nargs=1, help='Dataset to configure')

    parser_pause = subparsers.add_parser('pause', \
        help=configure_cmd.__doc__.strip())
    parser_pause.set_defaults(func=pause_cmd)
    parser_pause.add_argument('dataset', nargs='?', \
        help='Dataset to configure, defaults to all')
    parser_pause.add_argument('--unpause', action='store_true', \
        help='Removes the pause flag')

    parser_clear = subparsers.add_parser('clear', \
        help=clear_cmd.__doc__.strip())
    parser_clear.set_defaults(func=clear_cmd)
    parser_clear.add_argument('dataset', nargs=1, \
        help='Dataset to clear, required')
    parser_clear.add_argument('--props', action='store_true', \
        help='Clear zback related properties')
    parser_clear.add_argument('--holds', action='store_true', \
        help='Remove send/recv holds placed on snapshots')
    parser_clear.add_argument('--snaps', action='store_true', \
        help='Destroy zback-created snapshots')
    parser_clear.add_argument('--full', action='store_true', \
        help='Remove all zback related data')

    parser_zabbix = subparsers.add_parser('zabbix', \
        help=zabbix_cmd.__doc__.strip())
    parser_zabbix.set_defaults(func=zabbix_cmd)

    args = parser.parse_args()

    # If verbose selected, add a stream handler
    if args.verbose:
        add_streamhandler(log)
        log.setLevel(logging.DEBUG)
    # Get all datasets
    dsets = get_zback_sets()
    args.filelogging = filelogging
    args.dsets = dsets
    # deal wiv it
    args.func(args)

if __name__ == '__main__':
    main()
