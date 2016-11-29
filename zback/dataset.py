import subprocess
import datetime
import logging
import utils
import snapshot

class Dataset(object):
    '''
    Dataset properties and operations
    '''

    def __init__(self, name):
        self.log = logging.getLogger('witback.dataset')

        self.name = name
        self.snapshot = None
        self.snaplist = []
        self.retention = None
        self.destinations = None
        self.progress = None
        self.generic_get = 'zfs get -H -o value {0}:{1} {2}'

    def __getstate__(self):
        # Remove logger so can be pickled
        d = dict(self.__dict__)
        del d['log']
        return d

    def get_properties(self):

        # Does all of the properties at once - this way it can be threaded more efficiently

        # NOTE - This is probably best not hardcoded huh...

        property_commands = ['zfs get -H -o value org.wit:snapshot {0}',
                             'zfs get -H -o value org.wit:retention {0}',
                             'zfs get -H -o value org.wit:destinations {0}',
                             'zfs list -H -t snap -r {0} -o name']

        try:
            prop_out = []
            for cmd in property_commands:
                prop_out.append(utils.run_command(cmd.format(self.name)))
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            raise

        if prop_out[0].rstrip() == 'yes':
            self.snapshot = True
        else:
            self.snapshot = False

        if prop_out[1].rstrip() != '-':
            try:
                schema = utils.sched_from_schema(prop_out[1].rstrip())
                self.retention = schema
            except ValueError as e:
                self.log.debug(e)
                raise
        else:
            raise ValueError("Need retention for dataset{0}".format(self.name))

        if prop_out[2].rstrip() != '-':
            dst_list = utils.parse_destinations(prop_out[2].rstrip())
            if dst_list:
                self.destinations = dst_list
            else:
                self.log.debug("No destinations set for dataset {0}".format(self.name))
        else:
            self.log.debug("No destinations set for dataset {0}".format(self.name))

        if len(prop_out) > 3:
            if prop_out[3] is not None:
                self.snaplist = []
                for snap in prop_out[3].split():
                    this_snap = snapshot.Snapshot(snap.rstrip())
                    self.snaplist.append(this_snap)
        else:
            self.log.debug("No snapshots found for this dataset")

    def take_snapshot(self):
        # Takes a snapshot with pre-formatted name

        now = datetime.datetime.now()
        now_name = utils.name_from_date(now)
        command = 'zfs snap {0}@{1}'.format(self.name, now_name)

        try:
            utils.run_command(command)
            self.log.debug("Snapshot completed successfully for {0}".format(self.name))
        except subprocess.CalledProcessError as e:
            self.log.debug(e)
            raise

    def send(self, latest_remote, channel, ref):
        '''
        Sends the latest local snapshot through mbuffer to the remote end. If
        port is specified mbuffer outputs to that port, assuming it will be tunneled
        over and SSH connection.
        '''
        try:
            self.snaplist.sort(key=lambda item:item.date)
            latest_local = self.snaplist.pop()
        except:
            self.log.error("Could not get latest local snapshot for dataset {0}".format(self.name))
            raise

        if latest_local.name == latest_remote:
            self.log.info("Remote snapshot up to date for dataset {0}".format(self.name))
            return
        
        send_cmd = 'zfs send -i {0} {1}'.format(latest_remote, latest_local.name)
#        pipe_cmd = 'mbuffer'

        send = subprocess.Popen(send_cmd.split(), stdout=subprocess.PIPE)
#        pipe = subprocess.Popen(pipe_cmd.split(), stdin=send.stdout, stderr=subprocess.PIPE)

        while send.returncode is None:
            send.poll()
            for line in send.stdout:
                channel.send(line)

        if send.returncode == 0:
            channel.close()
            return
        else:
            self.log.debug("Sending snapshot {0} failed with returncode {1}".format(
                latest_local.name, send.returncode))
            raise RuntimeError("Send failed")

        # If snapshot sent successfully, put a hold on it
        try:
            latest_local.hold(ref)
        except TypeError:
            self.log.warning("Could not hold snapshot {0}".format(latest_local.name))
        except subprocess.CalledProcessError:
            self.log.warning("Could not hold snapshot {0}".format(latest_local.name))
        
       # Remove any hold with this ref on it for any other snapshot

        held_snaps = [snap for snap in self.snaplist if snap.holds is not None]
        for snap in held_snaps:
            if ref in snap.holds:
                try:
                    snap.unhold(ref)
                except:
                    self.log.warning("Could not unhold snapshot {0}".format(snap.name))



    @staticmethod
    def get_datasets(return_all = False):
        '''
        Loops all available datasets and returns a list of dataset objects
        If return_all is True, returns root datasets, ie STORAGE as well as
        STORAGE/dataset
        '''
        dataset_list = []
        command = 'zfs list -H -o name'

        try:
            unfmt_datasets = utils.run_command(command)
        except subprocess.CalledProcessError as e:
            raise

        if not unfmt_datasets:
            raise RuntimeError("No datasets found")

        for dataset in unfmt_datasets.split():
            if not return_all:
                if len(dataset.split('/')) == 1:
                    continue
            this_set = Dataset(dataset)
            dataset_list.append(this_set)

        return dataset_list
