#!/usr/bin/env python3
from datetime import datetime
from threading import Thread
import subprocess
import argparse
import paramiko
import logging
import random
import signal
import socket
import json
import time
import sys
import re

# Globals, because I'm not messing around with loading config files. If you want
# to change these go ahead.
PROP='test:'
TIMEFMT='%Y-%m-%d-%M%S'
DEFTAGS={'hourly' : '12', \
         'daily' : '7', \
         'weekly' : '6', \
         'monthly' : '6'}
ZBX_SEND=True
HELP='''
     '''

def zbx_send(dsname, prop, value):
    '''
        Dummy function for now, simply prints
    '''
    if not ZBX_SEND:
        return
    print("Sort out zabbix sending")

def get_all_datasets(disco=False):
    '''
        Returns a list of all datasets
    '''
    CMD = ['zfs', 'list', '-H', '-o', 'name']
    try:
        datasets = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                           universal_newlines=True)
    except subprocess.CalledProcessError as e:
        self.log.error(e.output)
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
    def __init__(self, dataset, run_tag):
        Thread.__init__(self)
        self.dataset = dataset
        self.run_tag = run_tag
        self.log = logging.getLogger('zback.{0}'.format(self.dataset.name))
        self._get_send_tag()
    def _get_send_tag(self):
        '''
        Updates the local send tag 
        '''
        try:
            self.send_tag = self.dataset.props['send']
        except KeyError as e:
            self.log.debug(e)
            self.log.info('No send tag specified for dataset %s', self.dataset.name)
            self.send_tag = None
    def _remote_command(self, command):
        '''
        Runs a remote command and returns the output or error
        '''
        try:
            cin, cout, cerr = self.sshclient(command)
            cout_fmt = cout.read().decode()
            cerr_fmt = cerr.read().decode()
            return (cout_fmt, cerr_fmt)
        except paramiko.SSHException as e:
            self.log.debug(e)
            self.log.debug('Error processing command: %s', command)
            raise
        except TypeError:
            # Probably means the sshclient hasn't been initialised, full panic
            raise
    def _get_remote_snaplist(self):
        '''
        Attempts to get the latest remote snapshot
        '''
        try:
            remote_snap = 'zfs list -H -o name -t snap -s creation '\
                         '-r {0}'.format(dest_fmt[-1])
            rsnapout, rsnaperr = self._remote_command(remote_snap)
            # Important to constrain ourselves to only the tag we're running
            # with. Given we are at this point we can assume that this is a send
            # and thus that the tag that is being sent is the only one meant to
            # be kept in sync with the opposite end.
            self.remote_snaplist = [snap for snap in \
                                    rsnapout.rstrip('\n').split('\n') \
                                    if PROP in snap and self.run_tag in snap]
            self.latest_remote = remote_snaplist.pop()
        except IndexError:
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
            except paramiko.SSHException as e:
                self.log.debug(e)
                self.log.warning('Could not set up netcat on remote' \
                                ' end, aborting')
                attempts-=1
                continue
            except socket.timeout as e:
                self.log.debug(e)
                self.log.warning('Timed out waiting for nc command')
                attempt-=1
                continue
        # If 5 attempts, assume something screwy is going on and bail
        raise RuntimeError('Could not establish nc listener on remote after ' \
                           'multiple attempts')
    def _send(self, destination):
        '''
        Wraps as many send functions as there are protocols. Some functionality
        is common to different protocols. 
        '''
        # Get the list of held snapshots
        try:
            self.dataset.get_holdlist()
            for snap in self.dataset.holdlist:
                hold = self.dataset.get_hold(snap)
                if destination+':SEND' in hold:
                    self.log.error('Send in progress, aborting')
                    raise RuntimeError
        except IndexError:
            self.log.debug('No held snapshots')
            send_holds = None
        except subprocess.CalledProcessError:
            self.log.error('Error getting list of held snapshots')
            raise
        try:
            # If this run is a go, put a send hold on the latest
            # snapshot and send it
            self.dataset.hold_snap(self.latest_snap, destination+':SEND')
            self.log.debug('No send in progress for destination %s, ' \
                           'beginning send', destination)
            if 'local:' in destination:
                # Need to refresh snaplist for some reason, was bugging out
                # before...
                self.dataset.get_snaps()
                self._local_send(destination)
            elif 'nc:' or 'ssh:' in destination:
                self.dataset.get_snaps()
                self._remote_send(destination)
            # Presuming the send went ok, release the send hold
            self.dataset.release_snap(self.latest_snap, destination+':SEND')
            # Place a normal hold on the most recently sent snapshot
            self.dataset.hold_snap(self.latest_snap, destination)
        except Exception:
            self.dataset.release_snap(self.latest_snap, destination+':SEND')
        # Check the rest of the hold list to see if any more holds for this
        # destination are present. Note that this works provided the hold list
        # has not been updated which is why the update is only ever called from
        # the beginning of the send function
        for hold in self.dataset.holdlist:
            try:
                if self.dataset.get_hold(hold) == destination:
                    self.dataset.release_snap(hold, destination)
            except subprocess.CalledProcessError:
                self.log.error('Issue removing hold on old snapshots')
                raise
    def _remote_send(self, destination):
        '''
        Further wrapping around the case where the send is to a remote server.
        This presumes that ssh is set up and working, and that there is a
        passwordless key in place to access the remote server. At some point I
        may consider making this more complicated but I doubt it.
        '''
        dest_fmt = destination.split(':')
        # Try to establish an ssh connection to the remote host. Catch the many
        # wonderful possible ways this could go wrong and don't bother doing
        # anything about them as this is not our problem.
        try:
            self.log.debug('Trying to connect to remote host via ssh')
            self.sshclient = paramiko.client.SSHClient()
            self.sshclient.load_system_host_keys()
            self.sshclient.connect(dest_fmt[1])
        except paramiko.BadHostKeyException as e:
            self.log.debug(e)
            self.log.error('Could not connect to host %s, ' \
                            'bad host key', dest_fmt[1])
            raise
        except paramiko.AuthenticationException as e:
            self.log.debug(e)
            self.log.error('Could not connect to host %s, ' \
                            'authentication failure', dest_fmt[1])
            raise
        except paramiko.SSHException as e:
            self.log.debug(e)
            self.log.error('Could not connect to host %s, ' \
                            'check logs', dest_fmt[1])
            raise
        except socket.error as e:
            self.log.debug(e)
            self.log.error('Socket error while connecting to ' \
                            '%s', dest_fmt[1])
            raise
        except IndexError as e:
            self.log.debug(e)
            self.log.debug('Dest string: %s', destination)
            self.log.error('Destination incorrectly formatted for ' \
                            'dataset %s', self.dataset.name)
            raise
        except IOError as e:
            self.log.debug(e)
            self.log.error('Could not load system host keys')
            raise
        # Check latest snapshot on remote side
        self._get_remote_snaplist()
        # Find the corresponding local snapshot and thus build the send command
        # that we will need
        if self.latest_remote:
            try:
                # Match the timestamps
                local_match = [snap for snap in self.dataset.snaplist \
                               if snap.split('@')[-1] == \
                               self.latest_remote.split('@')[-1]].pop()
                send_cmd = ['zfs', 'send', '-c', '-i', local_match, \
                            self.dataset.snaplist.pop()]
            except IndexError as e:
                # Implies no common snapshots between replication pair, full
                # error, abort
                self.log.debug(e)
                self.log.error('Snapshots out of sync for dataset ' \
                               '%s', self.dataset.name)
                raise
        else:
            send_cmd = ['zfs', 'send', '-c', self.dataset.snaplist.pop()]
        # Note that this can be modified in future to potentially incorporate
        # send and receive resuminging. This might be possible with the
        # _remote_command function
        recv_cmd = '/sbin/zfs recv {0}'.format(dest_fmt[-1])
        try:
            port = self._nc_listen(recv_cmd)
            time.sleep(1)
        except Exception as e:
            self.log.error(e)
            raise
        try:
            if dest_fmt[0] == 'nc':
                self.log.debug('Beginning direct send')
                self._direct_send(send_cmd, dest_fmt, port)
            elif dest_fmt[0] == 'ssh':
                self.log.debug('Beginning SSH send')
                self._ssh_send(send_cmd, dest_fmt, port)
        except Exception as e:
            self.log.debug(e)
            self.log.error("Error sending snapshot")
            self.sshclient.close()
        # Assuming the send went ok, need to try to hold the remote snapshot as
        # well to ensure that it does not get rotated out as well
        try:
            self._get_remote_snaplist()
            self._hold_remote_snap(self.remote_snaps.pop())
            # This presumes that the previous snapshot, prior to the most
            # recently send, is held. This should be the case, if it is not then
            # something has gone wrong but this possibly isn't the best way of
            # handling it.
            self._release_remote_snap(self.remote_snaps.pop())
        except Exception:
            raise
    def _local_send(self, destination):
        self.log.debug('Beginning local send run')
        dest_fmt = destination.split(':')
        try:
            dest_cmd = ['zfs', 'list', '-t', 'snap', '-o', 'name', \
                        '-H', '-s', 'creation', '-r',  dest_fmt[-1]]
            self.log.debug(dest_cmd)
            latest_dest = subprocess.check_output(dest_cmd).strip('\n').split('\n', \
                                                  universal_newlines=True).pop()
            latest_match = [snap for snap in self.dataset.snaplist \
                            if snap.split('@')[-1] == \
                            latest_dest.split('@')[-1]].pop()
            if not latest_match:
                raise IndexError
            send_cmd = ['zfs', 'send', '-i', latest_match, \
                        self.dataset.snaplist.pop()]
            recv_cmd = ['zfs', 'recv', dest_fmt[-1]]
        except IndexError as e:
            self.log.debug(e)
            self.log.warning('No snaphots in common for ' \
                             '%s and %s', dest_fmt[-1], self.dataset.name)
            send_cmd = ['zfs', 'send', self.dataset.snaplist.pop()]
            recv_cmd = ['zfs', 'recv', '-F', dest_fmt[-1]]
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            self.log.error('Could not check destination filesystem: ' \
                           '%s', dest_fmt[-1])
            raise
        try:
            send = subprocess.Popen(send_cmd, stdout=subprocess.PIPE, \
                                    universal_newlines=True)
            recv = subprocess.Popen(recv_cmd, stdin=send.stdout, \
                                    universal_newlines=True)
            while recv.poll() is None:
                time.sleep(1)
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            self.log.error('Error sending snapshot')
            raise
        self.log.debug('Snapshot sent successfully')
    def _direct_send(self, send_cmd, dest_fmt, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((dest_fmt[1], port))
            self.log.debug('nc listen established, socket connected')
        except socket.error as e:
            self.log.debug('Socket error: '+e)
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
        except subprocess.CalledProcessError as e:
            self.log.debug(send_cmd)
            self.log.error(e.output)
            self.log.debug(e)
            self.log.error('Send command failed for %s, ' \
                            'check logs', self.latest_snap)
            raise
    def _ssh_send(self, send_cmd, dest_fmt, port):
        self.log.debug('SSH send requested, attempting to open channel')
        transport = self.sshclient.get_transport()
        try:
            # This will fail if there is nothing listening on the other end
            channel = transport.open_channel('direct-tcpip', \
                                             dest_addr=(dest_fmt[1], port), \
                                             src_addr=('localhost', port))
            self.log.debug('SSH channel opened to nc listen on port ' \
                           '%s', port)
        except paramiko.SSHException as e:
            self.log.debug(e)
            self.log.error('Could not open SSH forward channel')
            raise
        try:
            self.log.debug('Beginning send command')
            self.log.debug('send_cmd: %s', send_cmd)
            sendproc = subprocess.Popen(send_cmd, stdout=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            self.log.debug('send_cmd: %s', send_cmd)
            self.log.error('Could not start local send operation')
            raise
        while True:
            data = sendproc.stdout.read(4096)
            if data: 
                channel.send(data)
            else: 
                break
        self.log.debug('Send finished, closing connections')
        channel.close()
    def _prune(self):
        self.log.info('Beginning prune run')
        try:
            # Extract tags from snapshot
            raise KeyError
        except KeyError as e:
            self.log.debug(e)
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
        self.log.debug('Pruning complete')
        return 0
    def run(self):
        '''
        This is called by thread.start(), processes the options applied to the
        dataset and decides what to do accordingly
        '''
        try:
            if self.send_tag is None:
                # No send tag specified so just snap and prune
                self.log.debug('No send tag, snap and prune only')
                # Simply take a snapshot, with the tag from the overall run and then
                # prune any snapshots that need to be deleted
                self.log.debug('Taking snapshot...')
                self.dataset.take_snap(tag=self.run_tag)
                self.log.debug('Pruning old snapshots')
                self._prune()
                return 0
            else:
                # If we're taking but not sending on this particular run then
                # simply go ahead and do that
                if not self.send_tag == self.run_tag:
                    self.log.debug('Not a send run')
                    self.log.debug('Taking snapshot...')
                    self.dataset.take_snap(tag=self.run_tag)
                    self.log.debug('Pruning old snapshots')
                    self._prune()
                    return 0
                # If this is a send run then we need to do a lot more work
                self.log.debug('Checking latest local snap')
                latest_snap = self.dataset.snaplist.pop()
                try:
                    # Get list of destinations - if only one this list will
                    # simply consist of one element
                    self.log.debug('Checking destination')
                    dest = self.dataset.props['destination'].split(',')
                    self.log.debug(dest)
                except KeyError as e:
                    # This implies a config error as a send tag has been
                    # specified but no destination set
                    self.log.debug(e)
                    self.log.error('No destination set for dataset %s, ' \
                                   'but send tag has been set, check ' \
                                   'config', self.dataset.name)
                    # Do a normal run anyway
                    self.dataset.take_snap(tag=self.run_tag)
                    self._prune()
                    return 1
                # At this point we are assuming that there isn't a send in
                # progress and we have nearly all of the info we need.
                # First, take a snapshot, then get the new latest snapshot
                self.log.debug('Taking snapshot...')
                self.dataset.take_snap(tag=self.run_tag)
                self.previous_snap = latest_snap
                self.log.debug('Snapshot rearrange')
                self.latest_snap = self.dataset.snaplist.pop()
                # Now we pass this info onto the send wrapper, which can be
                # modified in future if I feel like supporting any more
                # transport protocols
                self.send_fail = False
                self.log.debug('Beginning send runs')
                for destination in dest:
                    try:
                        self._send(destination)
                    except Exception as e:
                        self.log.debug(e)
                        self.log.warning('Could not hold latest snapshot for %s, ' \
                                         'assuming send in' \
                                         'progress', self.dataset.name)
                        self.send_fail = True
                        continue
                    # Run a separate send for each destination, continuing if
                    # any errors
                if self.send_fail:
                    return 1
                else:
                    return 0
        except KeyError as e:
            # Key error when destination not specified, need to clean up and
            # exit - technically this is a failure as it implies send has been
            return 1
        except IndexError as e:
            # Catch the case where there are no snapshots present for the
            # filesystem. This will require there to be a way to initialise a
            # system easily.
            # TODO - Sort out initial run
            return 1
        except subprocess.CalledProcessError:
            # Catch the case where either the bog standard snap or prune
            # operations fail. Note that this only covers those cases where zfs
            # functions are encountered, so this indicates cases where
            # underlying zfs is not responding correctly. This will cause an
            # error to be logged at a lower level.
            return 1
        finally:
            self._prune()

class Dataset(object):
    '''
        Dataset and related operations
    '''
    def __init__(self, name):
        self.name = name
        self._get_props()
        self.get_snaps()
        self.log = logging.getLogger('zback.{0}'.format(self.name))
    def _get_props(self):
        CMD = ['zfs', 'get', 'all', '-H', \
               '-o', 'property,value', self.name]
        try:
            all_props = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                                universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            self.log.error(e.output)
            raise
        self.props = {re.sub(PROP, '', k):v for k,v in \
                     [prop.split('\t') for prop in all_props.split('\n') \
                     if PROP in prop]}
        try:
            assert self.props['snap']
        except (AssertionError, KeyError) as e:
            self.log.debug(e)
            raise
    def clear_holds(self):
        '''
            Very dangerous, only really here for testing
        '''
        # Seriously don't go doing this
        self.get_holdlist()
        [self.release_snap(snap, self.get_hold(snap)) for snap in self.holdlist]
        self.get_holdlist()
    def get_snaps(self):
        CMD = ['zfs', 'list', '-H', '-t', 'snap', '-s', \
               'creation', '-o', 'name', '-r', self.name]
        try:
            snapshots = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                                universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
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
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            self.log.error('Failed to get list of held snapshots for ' \
                           '%s', self.name)
            raise
        self.holdlist = [snap.split('\t')[0] for snap in getholds.split('\n') \
                         if '@' in snap and int(snap.split('\t')[-2])]
    def get_hold(self, snap):
        CMD = ['zfs', 'holds', '-H', snap]
        try:
            gethold = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                              universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            raise
        return gethold.split('\t')[1]
    def set_prop(self, prop, value):
        CMD = ['zfs', 'set', PROP+prop+'='+value, self.name]
        try:
            setprop = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                              universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
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
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            self.log.info('Failed to take snapshot: %s', snap)
            raise
    def destroy_snap(self, snap):
        CMD = ['zfs', 'destroy', snap]
        try:
            delsnap = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                              universal_newlines=True)
            self.get_snaps()
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            self.log.info('Failed to destroy snapshot: %s', snap)
            raise
    def hold_snap(self, snap, ref):
        CMD = ['zfs', 'hold', ref, snap]
        try:
            holdsnap = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                               universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            self.log.info('Failed to hold snapshot %s with %s', snap, ref)
            raise
    def release_snap(self, snap, ref):
        CMD = ['zfs', 'release', ref, snap]
        try:
            relsnap = subprocess.check_output(CMD, stderr=subprocess.PIPE, \
                                              universal_newlines=True)
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            self.log.info('Failed to release snapshot %s with %s', snap, ref)
            raise

def main():
    '''
        Parse args and decide what to do
    '''
    # Really need to figure out this logging shit
    log = logging.getLogger('zback')
    formatter = logging.Formatter('%(name)s::%(levelname)s: %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    # This needs sorting TODO
    parser = argparse.ArgumentParser(description=HELP)
    parser.add_argument('tag', \
                        nargs='?', \
                        help='Tag for this run', \
                        action='store')
    parser.add_argument('-v', \
                        '--verbose', \
                        action='store_true')
    parser.add_argument('-d', '--discover', \
                        help='JSON formatted list for Zabbix auto-discovery', \
                        action='store_true')
    args = parser.parse_args()
    dsets = []
    for dataset in get_all_datasets():
        try:
            dset = Dataset(dataset)
            dsets.append(dset)
            log.debug('Found dataset: {0}'.format(dest.name))
        except Exception:
            continue
    if not dsets:
        print('No datasets configured, nothing to do')
        sys.exit(0)
    disco_sets = [dset for dset in dsets if dset.props['snap'] == 'yes']
    if args.discover:
        print(json.dumps({'data':[{'#ELEMNAME':dset.name} for dset in disco_sets]}))
        sys.exit(0)
    if not args.tag:
        print('Must supply tag')
        sys.exit(1)
    for dset in disco_sets:
        this_dset_run = DatasetWorker(dset, args.tag)
        this_dset_run.start()

if __name__ == '__main__':
    main()
