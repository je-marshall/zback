import subprocess
import datetime
import logging
import utils
import snapshot

PREFIX = None

class Dataset(object):
    '''
    Dataset properties and operations
    '''

    def __init__(self, name):
        self.log = logging.getLogger('witback.dataset')

        self.name = name
        self.snapshot = None
        self.snaplist = None
        self.retention = None
        self.destinations = None
        self.generic_get = 'zfs get -H -o value {0}:{1} {2}'

    def __getstate__(self):
        # Remove logger so can be pickled
        d = dict(self.__dict__)
        del d['log']
        return d

    def get_snapshot(self):
        '''
        This is a one off as it does not need to be stored - only ever
        referenced on the first run of the scheduler and then just assumed
        to be the case - nothing else from this class gets run if this
        returns false...
        '''

        command = self.generic_get.format(PREFIX, 'backup', self.name)

        try:
            snap = utils.run_command(command)
        except subprocess.CalledProcessError as e:
            self.log.error(e)
            raise

        if snap.rstrip() != '-':
            if snap.rstrip() == 'yes':
                return True
            elif snap.rstrip() == 'no':
                return False
        else:
            return False

    def get_properties(self):

        # Does all of the properties at once - this way it can be threaded more efficiently

        # NOTE - This is probably best not hardcoded huh...

        property_commands = ['zfs get -H -o value org.wit:retention {0}',
                             'zfs get -H -o value org.wit:destinations {0}',
                             'zfs list -H -t snap -r {0} -o name']

        try:
            prop_out = []
            for cmd in property_commands:
                prop_out.append(utils.run_command(cmd.format(self.name)))
        except subprocess.CalledProcessError as e:
            self.log.error(e)
            raise

        if prop_out[0].rstrip() != '-':
            try:
                schema = utils.sched_from_schema(prop_out[0].rstrip())
                self.retention = schema
            except ValueError as e:
                self.log.error(e)
                raise
        else:
            raise ValueError("Need retention for dataset{0}".format(self.name))

        if prop_out[1].rstrip() != '-':
            dst_list = utils.parse_destinations(prop_out[1].rstrip())
            if dst_list:
                self.destinations = dst_list
            else:
                self.log.debug("No destinations set for dataset {0}".format(self.name))
        else:
            self.log.debug("No destinations set for dataset {0}".format(self.name))

        if len(prop_out) > 2:
            for snap in prop_out[2].split():
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
            self.log.error(e)
            raise
