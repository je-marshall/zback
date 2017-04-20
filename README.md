Zback
=====

Zback is a name chosen for tab-completion purposes. It is a ZFS snapshot management service, written principally because existing examples were not flexible enough for my use case. However it is still in the early testing stage and as such I would definitely not recommend using it in production. Heavy inspiration has been drawn from ZnapZend, and also technical implementation of a reverse forwarding ssh tunnel from ssh-tunnel.

It is based on a client/server model, using ssh as the transport protocol. The client and server both listen on a port on the loopback device (although this can be tweaked) and communication between the two is acheived by passing messages over an ssh transport to the loopback port. Instead of using simple subprocess commands, I have used Paramiko in order to tunnel all communication requests, as well as the eventual zfs send, over the same ssh connection - this is important at scale as the default config for ssh has a limit on the number of connections that can be opened at any one time, meaning large numbers of datasets can overwhelm the receiving end if each one requires multiple connections. This of course can be tweaked in the sshd config but I figured it would be easier and more elegant to take this approach on the basis of minimal config.

Currently message passing is done using pickled messages, however I plan to redo this using JSON. Also, the client currently runs on a unix socket, and I plan to change this to a threading TCP server so as to be able to eventually have a nice web interface for it, but this is a long term goal.

To install zback:

cd /srv/

Note this is just a suggestion and you can pick whichever directory you like.

Clone the repo and then:

cd zback

./install.sh

This will run the installer, which will pull down all the required dependencies for the script, and set them up in a python virtualenv, so as to avoid interfering with the system as a whole.

Once zback is installed, you will be able to simply use zback as a command to run it. For information about using zback generally, see here (TO BE DONE)

Before running it for the first time, you will need to ensure that an offsite seed has been made and exists on the other end. In future versions this will be facilitated within zback itself but for now you will have to do this manually.

If you are installing zback over the top of a previous ZnapZend install, then this should not be necessary - all that needs doing is setting the correct properties for each dataset that you want to back up, which is covered in zback:config

Once you are sure you have everything configured and that there is an offsite seed to send to, simply enable the relevant services (for more information about how zback works overall, see here) that you want to run and then start them:

systemctl start zback-client.service

systemctl start zback-server.service

Log files can be found in /srv/zback/log, one for each of the running services. In future versions, for debugging purposes, it will be possible to run the program with console output. 



For now the configuration utility is being rewritten so here are instructions to add necessary config settings manually.

The easiest way, if you want to set the defaults on a dataset would be to run:

DATASET=STORAGE/projects
DEST=dest-srv:OFFSITE/client/server/projects\|hourly
SNAP=yes
RET=24h30d0w3m

Obviously changing the values for DATSET and DEST. Then run:

zfs set org.wit:snapshot=$SNAP $DATASET
zfs set org.wit:destinations=$DEST $DATASET
zfs set org.wit:retention=$RET $DATASET

This will sort things out for one set - if you have multiple ones you want to set, you could run a loop.

In future versions, the org.wit will be customisable for your organistations chosen prefix.

