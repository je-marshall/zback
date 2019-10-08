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
        set_cmd = 'zfs set %s=%si %s' % PROP+prop, value, self.name
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
        self.snaplist = [snap for snap in snapshots.rstrip('\n').split('\n') \
                        if PROP in snap]
    def take_snap(self, tag):
        '''
        Takes a snapshot
        '''
        nowstamp = PROP + datetime.now().strftime(TIMEFMT) + ':%s' % tag
        take_cmd = 'zfs snap %s@%s' % self.name, nowstamp
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
        self.get_snaps()
    def hold_snap(self, snap, ref):
        '''
        Places a userref on a snapshot
        '''
        hold_cmd = 'zfs hold %s %s' % ref, snap
        self._exec_command(hold_cmd)
        # Not sure about this bit
        self.get_holdlist()
    def release_snap(self, snap, ref):
        '''
        Releases a hold placed on a snapshot
        '''
        release_cmd = 'zfs release %s %s' % ref, snap
        self._exec_command(release_cmd)
        # Not sure about this bit
        self.get_holdlist()
    def send(self):
        '''
        Something only a source dataset can do
        '''
        self.get_snaps()
        # Check this syntax
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
        if initialise: recv_cmd = 'zfs recv -F %s' % self.name
        else: recv_cmd = 'zfs recv %s' % self.name
        try:
            recvproc = subprocess.Popen(recv_cmd.split(), \
                                        stdin=sendproc.stdout)
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
    def __init__(self, name, logname, host, connection_type):
        super().__init__(name, logname)
        self.host = host
        self.connection_type = connection_type
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
    def _exec_command(self, command):
        '''
        Runs a remote command and outputs in the same way as the super class
        does, returns the stdout if possible and raises if not
        '''
        try:
            cmd_in, cmd_out, cmd_err = self.sshclient.exec_command(command)
            cmd_out_fmt = cmd_out.read().decode()
            cmd_err_fmt = cmd_err.read().decode()
            if cerr_fmt:
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
                self._exec_command(nc_cmd, timeout=5)
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
                attempt-=1
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
        if self.connection_type == 'nc':
            try:
                self.log.debug('attempting direct send')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.host, port))
                self.log.debug('direct connection established')
                return sock
            except socket.error as exception:
                self.log.debug(exception)
                raise ConnectionError('Direct socket connect failed')
        elif self.connection_type == 'ssh':
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
    def receive(self, sendproc, initialise=False):
        '''
        Takes a subprocess as input and reads from its stdout. If the initialise
        flag is set, it attempts a -F recv 
        '''
        if initialise: recv_cmd = 'zfs recv -F %s' % self.name
        else: recv_cmd = 'zfs recv %s' % self.name
        try:
            port = self._nc_listen(recv_cmd)
            conduit = self._get_connection(port)
            while sendproc.poll() is None:
                data = sendproc.stdout.read(4096)
                if data:
                    conduit.send(data)
                else:
                    break
        except ConnectionError:
            raise
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            raise
        self.log.info('Send successful')
        conduit.close()


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

class DatasetWorker(Thread):
    '''
    Intended to be called per dataset that needs handling. A run consists of
    taking a snapshot, sending it to a remote destination or destinations if
    specified and then cleaning up the snapshots that result. Each run only
    considers a single tag
    '''
    def __init__(self, dataset, run_tag, initialise, prune):
        Thread.__init__(self)
        self.dataset = dataset
        self.run_tag = run_tag
        self.initialise = initialise
        self.prune = prune
        self.log = logging.getLogger('zback.{0}'.format(self.dataset.name))
        self._get_send_tags()
        self._get_recv_tags()
        self.remote_snaplist = None
        self.latest_remote = None
        self.sshclient = None
    def _get_send_tags(self):
        '''
        Updates the local send tags
        '''
        try:
            send_tags = self.dataset.props['send'].split(',')
            # Split the tags out but only append if they are formatted correctly
            # which means having a tag:protocol:destination at least
            self.send_tags = [tag.split(':') for tag in send_tags \
                              if len(tag.split(':')) > 2 or None]
        except KeyError as exception:
            self.log.debug(exception)
            self.log.info('No send tags specified')
            self.send_tag = None
    def _get_recv_tags(self):
        '''
        Checks for any recv tags
        '''
        try:
            self.recv_tags = self.dataset.props['recv'].split(',')
        except KeyError:
            self.log.info('No receive tags specifed')
            self.recv_tags = None
    def _remote_command(self, command):
        '''
        Runs a remote command and returns the output or error
        '''
        try:
            self.log.debug('Running remote command: %s', command)
            cin, cout, cerr = self.sshclient.exec_command(command)
            cout_fmt = cout.read().decode()
            cerr_fmt = cerr.read().decode()
            return (cout_fmt, cerr_fmt)
        except paramiko.SSHException as exception:
            self.log.debug(exception)
            self.log.debug('Error processing command: %s', command)
            raise
        except TypeError as exception:
            # Probably means the sshclient hasn't been initialised, full panic
            self.log.error(exception)
            raise
    def _get_remote_snaplist(self, dest_fmt):
        '''
        Attempts to get the latest remote snapshot
        '''
        try:
            # Is this running too quickly to register the latest snapshot that
            # has just been sent?
            time.sleep(3)
            remote_snap = 'zfs list -H -o name -t snap -s creation ' \
                         '-r {0}'.format(dest_fmt[-1])
            self.log.debug(remote_snap)
            rsnapout, rsnaperr = self._remote_command(remote_snap)
            # Important to constrain ourselves to only the tag we're running
            # with. Given we are at this point we can assume that this is a send
            # and thus that the tag that is being sent is the only one meant to
            # be kept in sync with the opposite end.
            self.remote_snaplist = [snap for snap in \
                                    rsnapout.rstrip('\n').split('\n') \
                                    if PROP in snap and self.run_tag in snap]
            self.latest_remote = self.remote_snaplist.pop()
            self.log.debug('Latest remote snapshot: %s', self.latest_remote)
        except IndexError:
            self.log.debug('No remote snapshots found')
            self.latest_remote = None
    def _hold_remote_snap(self, snap):
        '''
        Attempts to place a hold on a remote snapshot
        '''
        try:
            # Not really sure what the hold ref should be but the name of the
            # sending dataset seems appropriate
            remote_hold = 'zfs hold {0} {1}'.format(self.dataset.name, snap)
            # Check how to verify if there is anything in standard error for
            # remote commands and log it to debug
            rholdout, rholderr = self._remote_command(remote_hold)
            if rholderr == '':
                return
            else:
                self.log.error('Could not place hold on remote snapshot %s', snap)
                self.log.debug(rholderr)
        except paramiko.SSHException:
            self.log.error('Could not place hold on remote snapshot %s', snap)
            raise
    def _release_remote_snap(self, snap):
        '''
        Attempts to release remote snapshot
        '''
        try:
            remote_rel = 'zfs release {0} {1}'.format(self.dataset.name, snap)
            rrelout, rrelerr = self._remote_command(remote_rel)
            if rrelerr == '':
                return
            else:
                self.log.error('Could not place hold on remote snapshot %s', snap)
                selg.log.debug(rrelerr)
        except paramiko.SSHException:
            self.log.error('Could not release remote snapshot %s', snap)
    def _nc_listen(self, command):
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
            NC_CMD = 'nc -l {0} -e "{1}"'.format(port, \
                                                 command)
            self.log.debug('nc cmd: {0}'.format(NC_CMD))
            try:
                # Set it up and send back the port that got used
                self.sshclient.exec_command(NC_CMD, timeout=5)
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
                attempt-=1
                continue
        # If 5 attempts, assume something screwy is going on and bail
        raise RuntimeError('Could not establish nc listener on remote after ' \
                           'multiple attempts')
    def _send(self, job):
        '''
        Wraps as many send functions as there are protocols. Some functionality
        is common to different protocols.
        '''
        # This is lazy AF but its easier than refactoring the lot and will prove
        # the concept works
        del(job[0])
        destination = ':'.join(job)
        try:
            for snap in self.dataset.holdlist:
                if destination+':SEND' in self.dataset.get_holds(snap):
                    self.log.error('Send in progress to destination %s, aborting', \
                                   destination)
                    raise RuntimeError
        except subprocess.CalledProcessError:
            self.log.error('Error checking holds on snapshots')
        except IndexError:
            pass
        try:
            # If this run is a go, put a send hold on the latest
            # snapshot and send it
            self.dataset.hold_snap(self.dataset.snaplist[-1], destination+':SEND')
            self.log.debug('No send in progress for destination %s, ' \
                           'beginning send', destination)
            if 'local:' in destination:
                # Need to refresh snaplist for some reason, was bugging out
                # before...
                self._local_send(destination)
            elif 'nc:' or 'ssh:' in destination:
                self._remote_send(destination)
            # Presuming the send went ok, release the send hold
            self.log.debug('Removing send hold')
            self.dataset.release_snap(self.dataset.snaplist[-1], destination+':SEND')
            # Place a normal hold on the most recently sent snapshot
            self.log.debug('Placing completed hold %s on %s', destination, \
                           self.dataset.snaplist[-1])
            self.dataset.hold_snap(self.dataset.snaplist[-1], destination)
        except Exception as exception:
            self.log.debug(exception)
            self.dataset.release_snap(self.dataset.snaplist[-1], destination+':SEND')
        # Check the rest of the hold list to see if any more holds for this
        # destination are present. Note that this works provided the hold list
        # has not been updated which is why the update is only ever called from
        # the beginning of the send function
        for held_snap in self.dataset.holdlist:
            try:
                for hold in self.dataset.get_holds(held_snap):
                    if hold == destination:
                        self.log.debug('Releasing old snap %s', held_snap)
                        self.dataset.release_snap(held_snap, destination)
            except subprocess.CalledProcessError:
                self.log.error('Issue removing hold on old snapshots')
                raise
        self.log.debug('Send run complete for destination %s', destination)
    def _local_send(self, destination):
        self.log.info('Local send run starting')
        dest_fmt = destination.split(':')
        try:
            dest_cmd = ['zfs', 'list', '-t', 'snap', '-o', 'name', \
                        '-H', '-s', 'creation', '-r',  dest_fmt[-1]]
            self.log.debug(dest_cmd)
            latest_dest = subprocess.check_output(dest_cmd, \
                          universal_newlines=True).strip('\n').split('\n').pop()
            latest_match = [snap for snap in self.dataset.snaplist \
                            if snap.split('@')[-1] == \
                            latest_dest.split('@')[-1]].pop()
            send_cmd = ['zfs', 'send', '-i', latest_match, \
                        self.dataset.snaplist.pop()]
            recv_cmd = ['zfs', 'recv', dest_fmt[-1]]
        except IndexError as exception:
            self.log.debug(exception)
            self.log.warning('No snaphots in common for ' \
                             '%s and %s', dest_fmt[-1], self.dataset.name)
            if not self.initialise:
                self.log.error('Snapshots out of sync for destination %s', dest_fmt[-1])
                raise RuntimeError('Snapshots out of sync')
            self.log.warning('Attempting to initialise destination dataset')
            send_cmd = ['zfs', 'send', self.dataset.snaplist.pop()]
            recv_cmd = ['zfs', 'recv', '-F', dest_fmt[-1]]
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.error('Could not check destination filesystem: ' \
                           '%s', dest_fmt[-1])
            raise
        except Exception as exception:
            self.log.debug(exception)
            raise
        try:
            send = subprocess.Popen(send_cmd, stdout=subprocess.PIPE, \
                                    universal_newlines=True)
            recv = subprocess.Popen(recv_cmd, stdin=send.stdout, \
                                    universal_newlines=True)
            while recv.poll() is None:
                time.sleep(1)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.error('Error sending snapshot')
            raise
        self.log.info('Local send run completed successfully')
    def _remote_send(self, destination):
        '''
        Further wrapping around the case where the send is to a remote server.
        This presumes that ssh is set up and working, and that there is a
        passwordless key in place to access the remote server. At some point I
        may consider making this more complicated but I doubt it.
        '''
        dest_fmt = destination.split(':')
        self.log.info('Send is to remote host, attempting to connect via ssh')
        # Try to establish an ssh connection to the remote host. Catch the many
        # wonderful possible ways this could go wrong and don't bother doing
        # anything about them as this is not our problem.
        try:
            self.sshclient = paramiko.client.SSHClient()
            self.sshclient.load_system_host_keys()
            self.sshclient.connect(dest_fmt[1])
        except paramiko.BadHostKeyException as exception:
            self.log.debug(exception)
            self.log.error('Could not connect to host %s, ' \
                            'bad host key', dest_fmt[1])
            raise
        except paramiko.AuthenticationException as exception:
            self.log.debug(exception)
            self.log.error('Could not connect to host %s, ' \
                            'authentication failure', dest_fmt[1])
            raise
        except paramiko.SSHException as exception:
            self.log.debug(exception)
            self.log.error('Could not connect to host %s, ' \
                            'check logs', dest_fmt[1])
            raise
        except socket.error as exception:
            self.log.debug(exception)
            self.log.error('Socket error while connecting to ' \
                            '%s', dest_fmt[1])
            raise
        except IndexError as exception:
            self.log.debug(exception)
            self.log.debug('Dest string: %s', destination)
            self.log.error('Destination incorrectly formatted for ' \
                            'dataset %s', self.dataset.name)
            raise
        except IOError as exception:
            self.log.debug(exception)
            self.log.error('Could not load system host keys')
            raise
        except Exception as exception:
            self.log.debug(exception)
            raise
        # Check latest snapshot on remote side
        self.log.debug('Checking latest remote snapshot')
        try:
            self._get_remote_snaplist(dest_fmt)
        except Exception as exception:
            self.log.debug(exception)
            raise
        # Find the corresponding local snapshot and thus build the send command
        # that we will need
        if self.latest_remote:
            try:
                # Match the timestamps
                local_match = [snap for snap in self.dataset.snaplist \
                               if snap.split('@')[-1] == \
                               self.latest_remote.split('@')[-1]].pop()
                self.log.debug('Latest common snapshot: %s', local_match)
                send_cmd = ['zfs', 'send', '-c', '-i', local_match, \
                            self.dataset.snaplist[-1]]
                recv_cmd = '/sbin/zfs recv {0}'.format(dest_fmt[-1])
            except IndexError as exception:
                # Implies no common snapshots between replication pair, full
                # error, aborts
                self.log.debug(exception)
                if not self.initialise:
                    self.log.error('Snapshots out of sync for destination %s', dest_fmt[-2])
                    raise RuntimeError
                self.log.warning('Attempting to initialise remote dataset')
                send_cmd = ['zfs', 'send', '-c', self.dataset.snaplist[-1]]
                recv_cmd = '/sbin/zfs recv -F {0}'.format(dest_fmt[-1])
        else:
            # No remote snapshot means we might be trying to initialise
            if not self.initialise:
                self.log.error('Snapshots out of sync for destination %s', dest_fmt[-2])
                raise RuntimeError
            self.log.warning('Attempting to initialise remote dataset')
            send_cmd = ['zfs', 'send', '-c', self.dataset.snaplist[-1]]
            recv_cmd = '/sbin/zfs recv -F {0}'.format(dest_fmt[-1])
        # Note that this can be modified in future to potentially incorporate
        # send and receive resuminging. This might be possible with the
        # _remote_command function
        try:
            port = self._nc_listen(recv_cmd)
            time.sleep(1)
        except Exception as exception:
            self.log.error(exception)
            raise
        try:
            # Actually send the snapshot
            if dest_fmt[0] == 'nc':
                self.log.debug('Beginning direct send')
                self._direct_send(send_cmd, dest_fmt, port)
            elif dest_fmt[0] == 'ssh':
                self.log.debug('Beginning SSH send')
                self._ssh_send(send_cmd, dest_fmt, port)
        except Exception as exception:
            self.log.debug(exception)
            self.log.error("Error sending snapshot")
            self.sshclient.close()
        # Assuming the send went ok, need to try to hold the remote snapshot as
        # well to ensure that it does not get rotated out as well
        try:
            self.log.debug('Holding remote snapshots')
            self._get_remote_snaplist(dest_fmt)
            self._hold_remote_snap(self.latest_remote)
            # This presumes that the previous snapshot, prior to the most
            # recently send, is held. This should be the case, if it is not then
            # something has gone wrong but this possibly isn't the best way of
            # handling it.
            self._release_remote_snap(self.remote_snaplist.pop())
        except Exception as exception:
            self.log.debug(exception)
            raise
        self.log.debug('Send finished, closing connections')
        self.sshclient.close()
    def _direct_send(self, send_cmd, dest_fmt, port):
        self.log.info('Direct send requested')
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((dest_fmt[1], port))
            self.log.debug('nc listen established, socket connected')
        except socket.error as exception:
            self.log.debug('Socket error: '+exception)
            self.log.error('Could not establish connection to remote ' \
                            'host %s, remote nc failure?', dest_fmt[1])
            raise
        sock_stdout = sock.makefile('w')
        try:
            self.log.debug('Beginning send command on local side')
            self.log.debug('send cmd: %s', send_cmd)
            proc = subprocess.Popen(send_cmd, stderr=subprocess.PIPE, \
                                    universal_newlines=True, stdout=sock_stdout)
            proc_stderr = proc.stderr.read()
            if proc_stderr:
                self.log.debug('Send command error: %s', proc_stderr)
                raise RuntimeError
            self.log.debug('Send finished')
        except subprocess.CalledProcessError as exception:
            self.log.debug(send_cmd)
            self.log.error(e.output)
            self.log.debug(exception)
            self.log.error('Send command failed for %s, ' \
                            'check logs', self.dataset.snaplist[-1])
            raise
        self.log.info('Direct send run completed successfully')
    def _ssh_send(self, send_cmd, dest_fmt, port):
        self.log.info('SSH send requested')
        transport = self.sshclient.get_transport()
        try:
            # This will fail if there is nothing listening on the other end
            channel = transport.open_channel('direct-tcpip', \
                                             dest_addr=(dest_fmt[1], port), \
                                             src_addr=('localhost', port))
            self.log.debug('SSH channel opened to nc listen on port ' \
                           '%s', port)
        except paramiko.SSHException as exception:
            self.log.debug(exception)
            self.log.error('Could not open SSH forward channel')
            raise
        try:
            self.log.debug('Beginning send command')
            self.log.debug('send_cmd: %s', send_cmd)
            sendproc = subprocess.Popen(send_cmd, stdout=subprocess.PIPE)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.debug('send_cmd: %s', send_cmd)
            self.log.error('Could not start local send operation')
            raise
        while True:
            data = sendproc.stdout.read(4096)
            if data:
                channel.send(data)
            else:
                break
        self.log.info('SSH send run completed successfully')
        channel.close()
    def _prune(self):
        self.log.info('Beginning prune run')
        try:
            # Extract tags from snapshot
            raise KeyError
        except KeyError as exception:
            self.log.info('No tags found for dataset %s ' \
                          'assuming defaults', self.dataset.name)
            if self.run_tag not in DEFTAGS.keys():
                self.log.error('Run called with unspecified ' \
                               'tag for %s', self.dataset.name)
                raise ValueError
            tags = DEFTAGS
        self.dataset.get_snaps()
        try:
            relevant_snaps = [snap for snap in self.dataset.snaplist \
                              if PROP in snap]
        except IndexError:
            self.log.debug('No snapshots to prune')
            return 0
        del_list = []
        for tag in tags.keys():
            # This is dependent on the snaplist being correctly sorted
            try:
                this_tag_list = [snap for snap in relevant_snaps \
                                 if tag in snap]
                while len(this_tag_list) > int(tags[tag]):
                    del_list.append(this_tag_list.pop(0))
            except ValueError:
                self.log.error('Incorrectly formatted tag: %s', tags[tag])
                return 1
        for snap in del_list:
            try:
                self.log.debug('Deleting snapshot: %s', snap)
                self.dataset.destroy_snap(snap)
            except subprocess.CalledProcessError:
                continue
        self.log.info('Pruning complete')
        return 0
    def run(self):
        '''
        This is called by thread.start(), processes the options applied to the
        dataset and decides what to do accordingly
        '''
        # First we need to check if there are any receive tags specified - if
        # there are, and any of them match the current run tag, then we won't be
        # taking a snapshot, simply pruning and exiting
        self.log.info('Beginning run with tag %s', self.run_tag)
        if self.recv_tags:
            if self.run_tag in self.recv_tags:
                self.log.info('This tag is being received, no snapshot needed')
                self._prune()
                return
        # At this point we can take a snapshot
        self.log.info('Taking snapshot...')
        self.dataset.take_snap(tag=self.run_tag)
        # Now we need to see if we are doing any sending
        send_jobs = [tag for tag in self.send_tags if self.run_tag in tag]
        if send_jobs:
            # Update the list of held snapshots
            self.dataset.get_holdlist()
            self.log.info('Send tags specified')
            for job in send_jobs:
                try:
                    self._send(job)
                except Exception as exception:
                    # Blanket exception catching for unexpected log benefit
                    self.log.error('Send failed for %s', job)
                    self.log.debug(exception)
                    continue
        # Finally, run a prune anyhow, taking into account whether the sends
        # failed or not
        self._prune()

class Dataset():
    '''
    Dataset and related operations
    '''
    def __init__(self, name, verbose):
        self.name = name
        self.props = None
        self._get_props()
        self.verbose = verbose
        self.log = logging.getLogger('zback.{0}'.format(self.name))
        self._setup_logging()
        self.get_snaps()
        self.holdlist = None
    def _setup_logging(self):
        try:
            logfile = LOG_DIR+self.name.split('/')[-1]+'.log'
            formatter = logging.Formatter('%(asctime)s::%(levelname)s: %(message)s')
            handler = WatchedFileHandler(logfile)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            if self.verbose:
                self.log.setLevel(logging.DEBUG)
            else:
                self.log.setLevel(logging.INFO)
        except FileNotFoundError:
            raise
    def _get_props(self):
        CMD = ['zfs', 'get', 'all', '-H', \
               '-o', 'property,value', self.name]
        try:
            all_props = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                                universal_newlines=True)
        except subprocess.CalledProcessError:
            raise
        self.props = {re.sub(PROP, '', k):v for k,v in \
                     [prop.split('\t') for prop in all_props.split('\n') \
                     if PROP in prop]}
        try:
            if not self.props['manage'] == 'yes':
                raise AssertionError
        except KeyError:
            raise
    def _clear_holds(self):
        '''
            Very dangerous, only really here for testing
        '''
        # Seriously don't go doing this
        self.get_holdlist()
        for held_snap in self.holdlist:
            holds = self.get_holds(held_snap)
            for hold in holds:
                try:
                    self.release_snap(held_snap, hold)
                except subprocess.CalledProcessError as exception:
                    self.log.error('Failed to clear hold on snap %s', held_snap)
                    self.log.debug(exception)
                    continue
    def get_snaps(self):
        CMD = ['zfs', 'list', '-H', '-t', 'snap', '-s', \
               'creation', '-o', 'name', '-r', self.name]
        try:
            snapshots = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                                universal_newlines=True)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.error('Failed to get snapshot for %s', self.name)
            raise
        # Doesn't need to be sorted in python as it has been sorted by creation
        # time anyway
        self.snaplist = [snap for snap in \
                         snapshots.rstrip('\n').split('\n') \
                         if PROP in snap]
    def get_holdlist(self):
        CMD = ['zfs', 'get', 'userrefs', '-H', '-r', self.name]
        try:
            getholds = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                               universal_newlines=True)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.error('Failed to get list of held snapshots for ' \
                           '%s', self.name)
            raise
        self.holdlist = [snap.split('\t')[0] for snap in getholds.split('\n') \
                         if '@' in snap and int(snap.split('\t')[-2])]
    def get_holds(self, snap):
        CMD = ['zfs', 'holds', '-H', snap]
        try:
            gethold = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                              universal_newlines=True)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            raise
        return [hold.split('\t')[1].strip() for hold in gethold.strip('\n').split('\n')]
    def set_prop(self, prop, value):
        CMD = ['zfs', 'set', PROP+prop+'='+value, self.name]
        try:
            setprop = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                              universal_newlines=True)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.info('Failed to set property %s on ' \
                           'dataset %s', prop, self.name)
            raise
        self._get_props()
    def take_snap(self, tag=''):
        nowstamp = PROP+datetime.now().strftime(TIMEFMT)+':'+tag
        CMD = ['zfs', 'snap', '{0}@{1}'.format(self.name, nowstamp)]
        try:
            takesnap = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                               universal_newlines=True)
            self.log.debug('take_snap: %s@%s', self.name, nowstamp)
            self.get_snaps()
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            raise
    def destroy_snap(self, snap):
        CMD = ['zfs', 'destroy', snap]
        try:
            delsnap = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                              universal_newlines=True)
            self.get_snaps()
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.info('Failed to destroy snapshot: %s', snap)
            raise
    def hold_snap(self, snap, ref):
        CMD = ['zfs', 'hold', ref, snap]
        try:
            holdsnap = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                               universal_newlines=True)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.info('Failed to hold snapshot %s with %s', snap, ref)
            raise
    def release_snap(self, snap, ref):
        CMD = ['zfs', 'release', ref, snap]
        try:
            relsnap = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                              universal_newlines=True)
        except subprocess.CalledProcessError as exception:
            self.log.debug(exception)
            self.log.info('Failed to release snapshot %s with %s', snap, ref)
            raise

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
