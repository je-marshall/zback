import utils
import logging
import subprocess
import datetime

class Snapshot:
        '''
        Stores properties and methods pertaining to snapshots
        '''

        def __init__(self, name):
            self.log = logging.getLogger('witback.snap')
            
            self.name = name
            self.datestamp = None
            self.userrefs = None

        def get_date(self):

            command = 'zfs get -H creation {0}'.format(self.name) 

            try:
                unfmt_date = utils.run_command(command)
            except subprocess.CalledProcessError as e:
                self.log.error(e)
                raise

            date = unfmt_date.split('\t')[2]

            try:
                parse_date = utils.date_from_string(date)
            except:
                raise

            self.datestamp = parse_date

        def get_userrefs(self):

            tags = []
            command = 'zfs holds -H {0}'.format(self.name)
            
            try:
                unfmt_tags = utils.run_command(command)
            except subprocess.CalledProcessError as e:
                self.log.error(e)
                raise
            
            for unfmt_tag in unfmt_tags.splitlines():
                tags.append(unfmt_tag.split('\t')[1])

            if tags:
                self.log.debug("Tags found for snapshot {0}: {1}".format(self.name), tags)
                self.userrefs = tags
            else:
                self.log.debug("No tags found for snapshot {0}".format(self.name))
            
        def hold(self, ref):

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
