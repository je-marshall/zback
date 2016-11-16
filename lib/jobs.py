from operator import itemgetter
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
    for snap in sorted(dataset.snaplist, key=lambda x: x.date, reverse=True):
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

def send(dataset, location):
    '''
    Sends a snapshot to a location
    '''
    # Try to ssh into location, bail if any errors
    # Check if remote server is running
    # Ask for latest snapshot on remote end
    # Open port forward, ask remote end to start receive process
    # Begin sending into local end of port forward
    # Confirm snapshot received on remote end 

    log = logging.getLogger('witback.send')

    try:
        dataset.get_properties()
    except ValueError:
        log.error("Could not refresh dataset properties for dataset {0}".format(dataset.name))
        raise RuntimeError("Error getting dataset properties")
        
    # NOTE - this bit will need to be revised now that snaps are not dicts
    latest_local = sorted(dataset.snaplist, key=lambda x: x.date).pop()

    split_loc = location.split(':')
    if len(split_loc) == 1:
        try:
            check_cmd = 'zfs list -H -t snap -r {0} -o name'.format(location)
            check = utils.run_command(check_cmd)
        except subprocess.CalledProcessError:
            log.error("Incorrectly configured destination for dataset {0}, {1}".format(
                dataset.name, location))
            raise RuntimeError("Bad config for destination")

        latest_remote_unfmt = check.split().pop()
        latest_remote = dataset.name + '@' + lateset_remote_unfmt.split('@')[1]

        if latest_local == latest_remote:
            self.log.info("Destination {0} up to date")
            return

        send_cmd = 'zfs send -i {0} {1}'.format(latest_remote, latest_local)
        recv_cmd = 'zfs recv -F {0}'.format(location)

        local_send(send_cmd, recv_cmd)


        


    pass

def local_send