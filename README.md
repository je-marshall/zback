# zback

zback is a ZFS backup and replication utility. It draws heavy inspiration from
ZnapZend, zfs-auto-snapshot and zrep.

zback is designed to be run as a cron job. Each run is called with a tag, which
dictates snapshot names and whether sending is attempted, along with how many
local snapshots are kept. This is to allow for flexibility in sending schedules.
For example, the defaults assume that hourly, daily, weekly and monthly
snapshots will be being taken, and that some of these will be being sent.
However it is trivial to define your own tags, an example being frequent, run
every 10 minutes - the details are up to you.

An example cron job is included in the repository to give an idea of how to run
the script.

Also included are settings for the Zabbix monitoring platform which I'm not
going to explain how to set up or anything.

This software is in an early testing phase and has been running on CentOS 7
only, I accept literally no responsibility for it right now.

## Getting started

zback has been designed with minimalism in mind, but still has one requirement,
which is paramiko. To install, make sure pip3 is present:

> pip3 install paramiko

To flag a dataset as being managed by zback:

> zfs set zback:manage=yes $ZPOOL/$DATASET

If no other options are specified, zback will take a snapshot with the tag
supplied, and prune snapshots but not attempt a send.

To send a snapshot to another location, another ZFS option has to be set, but
before that a few things need to be considered. If the destination is a remote
server, then a passwordless SSH key should be installed and the host name you
want to connect to should be resolveable, there is no checking done to ensure
this is in place. Additionally, zback assumes that the user you are running as
has access to the server on the other end and will crap out dramatically if not.

An example send job would be:

> zfs set zback:send=hourly:ssh:remote-server1:REMOTEPOOL/REMOTEDATASET

This will attempt a send whenever the hourly run is called, to the server
remote-server1, sending the snapshot over an SSH channel, to the pool mentioned,
in the form `run_tag:protocol:remote-hostname:zpool/dataset`. 

Multiple send jobs are permitted, so long as they are comma-separated.

Supported remote protocols are tcp and ssh - in the case of ssh, the send is
channeled over a secure SSH connection. For hosts on a private network, I have
included support for a simple TCP connection to the remote server, with all the
security and performance implications that raises.

If the destination is a local dataset, then the format is:

> zfs set zback:send=local:DESTPOOL/DESTSET:hourly

Once both manage has been set and a send job has been configured, you should be
ready to go. To set up an initial push, run zback with the initalise flag:

> zback -i $ZPOOL/$DATASET -t $run_tag

This will attempt to send the snapshot to the remote side.

## Customisation

By default, if no tags are specified on a dataset then zback will assume the
following tags are going to be used:

> hourly:48
> daily:7
> weekly:6
> monthly:6

There are two ways that you can change this behaviour. One is to edit zback.py
and change the variable DEFTAGS, which will permanently alter this behaviour
across all datasets.

However, if you use zfs to set a property called tags, for example:

> zfs set zback:tags=frequent:5,hourly:24 $ZPOOL/$DATASET

Then this will override the defaults for this dataset only. The syntax ought to
be self-explanatory. 

One thing to note, if you are running zback on both sides of a sync, that on the
other end you will need to specify a recv flag, so as to prevent zback from
taking unnecessary snapshots. This can be done via:

> zfs set zback:recv=hourly $ZPOOL/$DATASET


