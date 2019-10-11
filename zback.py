#!/usr/bin/env python3
from logging.handlers import WatchedFileHandler
from datetime import datetime
from threading import Thread
import subprocess
import argparse
import logging
import random
import signal
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
PROP='zback:'
TIMEFMT='%Y-%m-%d-%H%M'
DEFTAGS={'hourly' : '48', \
         'daily' : '7', \
         'weekly' : '6', \
         'monthly' : '6'}
ZBX_SEND=True
ZBX_CONF='/etc/zabbix/zabbix-agentd.conf'
ZBX_BIN='/bin/zabbix_sender'
LOG_DIR='/var/log/zback/'
LOG_LVL=logging.WARNING
HELP='''
     '''

class NewDataset():
    '''
    Base Dataset class, implements local dataset handling functions. Is intended
    to be subclassed to provide remote functionality.
    '''
    def __init__(self, name, logname):
        self.name = name
        self.props = None
        self.log = logging.getLogger('zback.'.format(logname))
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
                                             stderr = subprocess.PIPE, \
                                             universal_newlines = True)
            self.log.debug('successful command: %s' % command)
        except subprocess.CalledProcessError:
            self.log.debug('failed command: %s' % command)
            self.log.error('Error running ZFS command')
            raise
        # TODO Figure out better subprocess.stderr handling
        return result
    def get_props(self):
        '''
        Updates the internal property dictionary
        '''
        prop_cmd = 'zfs get all -H -o property,value %s' % self.name
        all_props = self._exec_command(prop_cmd)
        self.props = {re.sub(PROP, '', k):v for k,v in \
                      [prop.split('\t') for prop in all_props.split('\n') \
                      if PROP in prop]}
    def set_prop(self, prop, value):
        '''
        Set a property
        '''
        set_cmd = 'zfs set %s=%s %s' % PROP+prop, value, self.name
        self._exec_command(set_cmd)
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
            send_cmd = ('zfs send -c -i %s %s' % increm, \
                        self.latest_snap).split()
        else:
            send_cmd = ('zfs send -c %s' % self.latest_snap).split()
        try:
            sendproc = subprocess.Popen(send_cmd, stdout=subprocess.PIPE, \
                                        universal_newlines=True)
            self.log.debug('successful cmd: %s' % send_cmd)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.error('Error running ZFS send command')
            raise
        # TODO Figure out better subprocess.stderr handling
        return sendproc
    def receive(self, source, initialise=False, increm=None):
        '''
        Receive for local dataset only, should be subclassed for remote
        '''
        if initialise: recv_cmd = 'zfs recv -F %s' % self.name
        else: recv_cmd = 'zfs recv %s' % self.name
        try:
            recvproc = subprocess.Popen(recv_cmd.split(), \
                                        stdin=source.send(increm=increm).stdout)
            while recvproc.poll() is None:
                time.sleep(1)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            raise
        self.log.info('Send successful')

class NewRemoteDataset(NewDataset):
    '''
    Subclassed to override a couple of functions. On initialisation attempts to
    establish an SSH connection to the remote side and will raise if not
    '''
    def __init__(self, name, host, protocol, logname):
        super().__init__(name, logname)
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
            self.sshclient.connect(self.host)
        except paramiko.BadHostKeyException as exception:
            self.log.debug(exception)
            self.log.error('Could not connect to host %s, ' \
                            'bad host key' % self.host)
            raise ConnectionError
        except paramiko.AuthenticationException as exception:
            self.log.debug(exception)
            self.log.error('Could not connect to host %s, ' \
                            'authentication failure' % self.host)
            raise ConnectionError
        except paramiko.SSHException as exception:
            self.log.debug(exception)
            self.log.error('Could not connect to host %s, ' \
                            'check logs' % self.host)
            raise ConnectionError
        except socket.error as exception:
            self.log.debug(exception)
            self.log.error('Socket error while connecting to ' \
                            '%s' % self.host)
            raise ConnectionError
        except IndexError as exception:
            self.log.debug(exception)
            self.log.debug('Dest string: %s' % destination)
            self.log.error('Destination incorrectly formatted for ' \
                            'dataset %s' % self.name)
            raise ConnectionError
        except IOError as exception:
            self.log.debug(exception)
            self.log.error('Could not load system host keys')
            raise ConnectionError
        except Exception as exception:
            self.log.debug(exception)
            raise
    def _exec_command(self, command, timeout=None):
        '''
        Runs a remote command and outputs in the same way as the super class
        does, returns the stdout if possible and raises if not
        '''
        try:
            cmd_in, cmd_out, cmd_err = self.sshclient.exec_command(command, timeout=timeout)
            cmd_out_fmt = cmd_out.read().decode()
            cmd_err_fmt = cmd_err.read().decode()
            if cmd_err_fmt:
                self.log.debug('stderr: %s' % cmd_err_fmt)
                self.log.warning('Remote command returned stderr')
                self.log.warning('remote command: %s' % command)
        except paramiko.SSHException as exception:
            self.log.debug(exception)
            self.log.error('Remote command failed')
            self.log.error('remote command: %s' % command)
            raise
        return cmd_out_fmt
    def _nc_listen(self, recv_command):
        '''
        Attempt to start a netcat listen process on the remote end. This is
        useful for either a direct send or a send via ssh. TODO - look into
        whitelisting for the direct send case or binding to localhost for the
        ssh send case
        '''
        attempts = 5
        while attempts >= 0:
            # Ephemeral ports, pretty unlikely to be used
            port = random.randint(32768, 60999)
            nc_cmd = 'nc -l {0} -e "{1}"'.format(port, \
                                                 recv_command)
            try:
                # Set it up and send back the port that got used
                self.sshclient.exec_command(nc_cmd, timeout=5)
                self.log.debug('nc listen established on port %s' % port)
                return port
            except paramiko.SSHException as exception:
                self.log.debug(exception)
                self.log.warning('Could not set up netcat on remote' \
                                ' end, aborting')
                attempts-=1
                continue
            except socket.timeout as exception:
                self.log.debug(exception)
                self.log.warning('Timed out waiting for nc command')
                attempts-=1
                continue
        # If 5 attempts, assume something screwy is going on and bail
        raise ConnectionError('Could not establish nc listener on remote after ' \
                              'multiple attempts')
    def _get_connection(self, port):
        '''
        Wraps _nc_listen and returns either a direct socket connection or a
        paramiko Channel that can be written to and responds to the same calls
        to close
        '''
        if self.protocol == 'nc':
            try:
                self.log.debug('attempting direct send')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.host, port))
                self.log.debug('direct connection established')
                return sock
            except socket.error as exception:
                self.log.debug(exception)
                raise ConnectionError('Direct socket connect failed')
        elif self.protocol == 'ssh':
            try:
                self.log.debug('attempting ssh send')
                transport = self.sshclient.get_transport()
                channel = transport.open_channel('direct-tcpip', \
                                                 dest_addr=(self.host, port), \
                                                 src_addr=('localhost', port))
                self.log.debug('ssh channel opened')
                return channel
            except paramiko.SSHException as exception:
                self.log.debug(exception)
                raise ConnectionError('SSH channel connect failed')
    def get_props(self):
        '''
        Overridden so as not to accidentally actually read the remote datasets
        properties as we don't care for them?
        '''
        pass
    def receive(self, source, initialise=False, increm=None):
        '''
        Takes a subprocess as input and reads from its stdout. If the initialise
        flag is set, it attempts a -F recv 
        '''
        if initialise: 
            recv_cmd = 'zfs recv -F %s' % self.name
        else: 
            recv_cmd = 'zfs recv %s' % self.name
        self.log.debug('recv_cmd: %s' % recv_cmd)
        try:
            self.log.debug('Setting up NC listener on remote')
            port = self._nc_listen(recv_cmd)
            # Quick sleep to allow nc to get ready
            time.sleep(1)
            self.log.debug('Establishing connection to remote port %i', port)
            conduit = self._get_connection(port)
            self.log.debug('Running send from source')
            sendproc = source.send(increm=increm)
            while sendproc.poll() is None:
                data = sendproc.stdout.read(4096)
                if data:
                    conduit.send(data)
        except ConnectionError:
            raise
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            raise
        except Exception as exception:
            self.log.debug(exception)
        self.log.info('Send successful')
        conduit.close()

class datasetProcessor(Thread):
    '''
    Accepts a local dataset and does a run on it. This consists of taking a
    snapshot if appropriate, sending if appropriate and pruning if appropriate
    '''
    def __init__(self, source, run_tag, initialise=False):
        Thread.__init__(self)
        self.source = source
        self.run_tag = run_tag
        self.initialise = initialise
        self.log = logging.getLogger('zback.{0}'.format(self.source.name))
        self.destination = None
    def _send_receive(self, job):
        '''
        '''
        if not self.run_tag in job:
            self.log.debug('Send job not for this run, skipping')
            return
        # Now that we've checked the tag for sending, strip it from the string
        # to leave only protocol and destination
        dest_str = job.lstrip('%s:' % self.run_tag)
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
        # This IS a send job, AND no sends are currently in progress so we can
        # go ahead and try to connect to the destination dataset, after putting
        # a send hold on
        self.log.debug('Placing send hold before starting send')
        self.source.hold_snap(self.sending_snap, send_hold)
        # Figure out what type of send we're doing and instantiate the right
        # class:
        if 'local:' in dest_str:
            self.log.debug('Destination is local')
            dest_name = dest_str.split(':')[-1], \
                self.destination = NewDataset(dest_name, self.source.name)
        else:
            self.log.debug('Destination is remote')
            protocol, host, dest_name = dest_str.split(':')
            self.destination = NewRemoteDataset(dest_name, host, protocol, \
                                                self.source.name)
        self.log.debug('Destination set up, checking latest matching snap')
        # None of the following have any try: excepts because they all need to
        # operate in sequence and if any of them fail they will be handled by
        # the run function.
        # Now that we have a destination dataset set up, we need to check their
        # respective snaplists:
        try:
            self.destination.get_snaps()
            if self.destination.latest_snap:
                src_match = [snap for snap in self.source.snaplist \
                            if snap.split('@')[-1] == \
                            self.destination.latest_snap.split('@')[-1]].pop()
            else:
                src_match = None
        except IndexError:
            self.log.debug('No common snapshots, will fail unless initialising')
            src_match = None    
        self.log.debug('src_match: %s', src_match)
        try:
            self.log.debug('Beginning send')
            self.destination.receive(self.source, initialise=self.initialise, \
                                     increm=src_match)
        except Exception as exception:
            self.log.debug(exception)
            # Doesn't matter what went wrong, pull the send hold and bail
            self.log.debug('Error during sending, attempting to release send hold')
            self.source.release_snap(self.sending_snap, send_hold)
            raise
        # Assuming that the send went correctly, then remove the send hold:
        self.source.release_snap(self.sending_snap, send_hold)
        # Place a successful sent hold on source and dest
        self.source.hold_snap(self.sending_snap, dest_str)
        # Destination hold reflects where it came from rather than its dest
        self.destination.get_snaps()
        self.destination.hold_snap(self.destination.latest_snap, \
                                   self.source.name)
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
                except:
                    continue
    def _prune(self):
        '''
        Using this run_tag, figures out how many snapshots need to be deleted
        and then does the necessary tidying up
        '''
        self.log.info('Beginning prune')
        try:
            tags = self.source.props['tags']
        except KeyError:
            self.log.debug('No tags specified, using defaults')
            tags = DEFTAGS
        if self.run_tag not in tags.keys():
            raise ValueError('Run tag does not match dataset tags or defaults')
        self.source.get_snaps()
        try:
            relevant_snaps = [snap for snap in self.source.snaplist \
                              if PROP in snap and self.run_tag in snap]
        except IndexError:
            self.log.debug('No snapshots to prune')
            return
        del_list = []
        while len(relevant_snaps) > int(tags[self.run_tag]):
            # Reverse pop because we want oldest snap
            del_list.append(relevant_snaps.pop(0))
        for snap in del_list:
            try:
                self.source.destroy_snap(snap)
            except subprocess.CalledProcessError:
                continue
        self.log.info('Pruning complete')
    def run(self):
        '''
        Performs a run on a dataset. 
        '''
        self.log.info('Beginning run with tag %s', self.run_tag)
        # NOTE possibly worth removing or clarifying when to setup logging
        self.source.get_props()
        self.log.debug(self.source.props)
        try:
            if self.run_tag in self.source.props['recv']:
                self.log.info('Tag received from source, no need to snap')
                self._prune()
                return
        except KeyError:
            self.log.debug('No recv tags specified')
        self.log.info('Taking snapshot...')
        self.source.take_snap(tag=self.run_tag)
        self.sending_snap = self.source.latest_snap
        try:
            send_tags = self.source.props['send'].split(',')
            send_jobs = [tag for tag in send_tags if self.run_tag in tag]
        except KeyError:
            self.log.debug('No send jobs specified')
            self._prune()
            return
        except Exception as exception:
            self.log.debug(exception)
            return
        if send_jobs:
            self.log.debug('send jobs: %s', send_jobs)
            self.source.get_holdlist()
            for job in send_jobs:
                try:
                    self._send_receive(job)
                except BlockingIOError:
                    continue
                except subprocess.CalledProcessError:
                    continue
                except ConnectionError:
                    continue
                except Exception as exception:
                    # Catch 'n log owt else
                    self.log.debug(exception)
                    continue
        self._prune()
        self.log.info('zback run completed')

def zbx_send(dsname, key, value, log):
    '''
    Sends updates to Zabbix. Does not check validity of global config variables.
    Can also be disabled by setting ZBX_SEND to False. In either that case or a
    successful send it returns, if it fails it returns a ConnectionError
    '''
    if not ZBX_SEND:
        return
    send_cmd = [ZBX_BIN, '-c', ZBX_CONF, \
                '-k', PROP.replace(':', '.')+key+'[%s]', dsname, \
                '-o', value]
    try:
        zabbix_send = subprocess.check_output(CMD, universal_newlines=True)
    except subprocess.CalledProcessError:
        log.debug('Failed sending results to zabbix server')
        log.debug('send_cmd: '+' '.join(send_cmd))
        log.warning('Error sending to zabbix, check config')
        raise ConnectionError


def get_all_datasets(disco=False):
    '''
    Returns a list of all datasets
    '''
    CMD = ['zfs', 'list', '-H', '-o', 'name']
    try:
        datasets = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                           universal_newlines=True)
    except subprocess.CalledProcessError as exception:
        self.log.error(exception.output)
        raise
    return [dset for dset in datasets.split('\n') \
            if len(dset.split('/')) > 1]


def main():
    '''
        Parse args and decide what to do
    '''
    parser = argparse.ArgumentParser(description=HELP)
    parser.add_argument('tag', \
                        nargs='?', \
                        help='Tag for this run', \
                        action='store')
    parser.add_argument('-d', '--dataset', \
                        nargs=1, \
                        help='Limit to one dataset', \
                        action='store')
    parser.add_argument('-p', '--prune', \
                        help='Prune this tag only', \
                        action='store_true')
    parser.add_argument('-i', '--initialise', \
                        help='Attempt to initialise destination datasets', \
                        action='store_true')
    parser.add_argument('-v', \
                        '--verbose', \
                        help='Prints debug output to console', \
                        action='store_true')
    parser.add_argument('-z', '--zabbix', \
                        help='JSON formatted list for Zabbix auto-discovery', \
                        action='store_true')
    args = parser.parse_args()
    if args.verbose:
        log = logging.getLogger('zback')
        formatter = logging.Formatter('%(asctime)s::%(name)s::%(levelname)s: %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        log.addHandler(handler)
        log.setLevel(logging.DEBUG)
    dsets = []
    for dataset in get_all_datasets():
        try:
            dset = Dataset(dataset, args.verbose)
            dsets.append(dset)
            if args.verbose:
                log.debug('Managing dataset: %s', dset.name)
        except (KeyError, AssertionError):
            if args.verbose:
                log.debug('Dataset %s not managed, skipping', dataset)           
            continue
        except FileNotFoundError:
            print('Logging directory not configured correctly')
            sys.exit(1)
        except subprocess.CalledProcessError:
            print('ZFS list command failed')
            sys.exit(1)
    if not dsets:
        print('No datasets configured, nothing to do')
        sys.exit(0)
    disco_sets = [dset for dset in dsets if dset.props['manage'] == 'yes']
    if args.zabbix:
        print(json.dumps({'data':[{'#ELEMNAME':dset.name} for dset in disco_sets]}))
        sys.exit(0)
    if not args.tag:
        print('Must supply tag')
        sys.exit(1)
    if args.dataset:
        this_dset = [dset for dset in dsets if args.dataset in dset]
        if this_dset:
            this_dset_run = DatasetWorker(dset, args.tag, \
                                          args.initialise, args.prune)
            this_dset_run.start()
            sys.exit(0)
    else:
        for dset in disco_sets:
            this_dset_run = DatasetWorker(dset, args.tag, \
                                          args.initialise, args.prune)
            this_dset_run.start()

if __name__ == '__main__':
    main()
