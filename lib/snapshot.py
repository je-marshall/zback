import logging
import subprocess
import utils

class Snapshot(object):
    '''
    Stores properties and methods pertaining to snapshots
    '''

    def __init__(self, name):
        self.log = logging.getLogger('witback.snap')
        self.name = name
        self.date = None
        self.holds = None

    def __getstate__(self):
        # Remove logger so can be pickled
        d = dict(self.__dict__)
        del d['log']
        return 

    def get_properties(self):
        '''
        All in one function to help with threading
        '''

        property_commands = ['zfs get -H creation -o value {0}',
                             'zfs holds -H {0}']

        try:
            prop_out = []
            for cmd in property_commands:
                prop_out.append(utils.run_command(cmd.format(self.name)))
        except subprocess.CalledProcessError as e:
            self.log.error(e)
            raise

        date = prop_out[0].rstrip()
        tags = []

        for unfmt_tag in prop_out[1].splitlines():
            tags.append(unfmt_tag.split('\t')[1])

        if tags:
            self.log.debug("Tags found for snapshot {0}: {1}".format(self.name, tags))
            self.holds = tags
        else:
            self.log.debug("No tags found for snapshot {0}".format(self.name))

        try:
            parse_date = utils.date_from_string(date)
        except ValueError as e:
            self.log.error(e)
            raise

        self.date = parse_date

    def hold(self, ref):
        '''
        Places a userref on the snapshot to prevent deletion
        '''

        try:
            reference = str(ref)
        except TypeError as e:
            self.log.error(e)
            raise

        command = 'zfs hold {0} {1}'.format(reference, self.name)

        try:
            hold_snap = utils.run_command(command)
        except subprocess.CalledProcessError as e:
            self.log.error(e)
            raise

        self.log.info("Hold {0} placed on snapshot {1}".format(reference, self.name))

    def unhold(self, ref):
        '''
        Removes a userref on the snapshot to allow deletion
        '''

        try:
            reference = str(ref)
        except TypeError as e:
            self.log.error(e)
            raise

        command = 'zfs release {0} {1}'.format(reference, self.name)

        try:
            unhold_snap = utils.run_command(command)
        except subprocess.CalledProcessError as e:
            self.log.error(e)
            raise

        self.log.info("Hold {0} removed on snapshot {1}".format(reference, self.name))

    def destroy(self):
        '''
        Destroys this Snapshot
        '''

        if self.holds is not None:
            raise RuntimeError("Snapshot held, cannot delete")

        command = 'zfs destroy {0}'.format(self.name)

        try:
            utils.run_command(command)
        except subprocess.CalledProcessError as e:
            self.log.error(e)
            raise