#!/bin/bash

# Temporary migration script to transfer from zrep + zfs-auto-snapshot to zback,
# needs to be run on primary and secondary

DATASET=$1
ZA_PREFIX="zfs-auto-snap_"
ZR_PREFIX="zrep_"
ZB_PREFIX="zback:"

SNAPLIST=$(zfs list -t snapshot -o name -r $DATASET)
ZA_SNAPS=()
ZR_SNAPS=()

rename_snap() 
{
    # Function to rename snapshots - quite complicated actually
    # Only applies to ZA snaps
    local SNAP=$1
    IFS="@"
    read -ra SNAPARR <<< "${1/$ZA_PREFIX/}"
    IFS=""
    STAMP=${SNAPARR[1]/[0-9]*/}
    NEWSNAP="$(echo ${SNAPARR[0]} | tr -d '\n';\
               echo "@" | tr -d '\n';\
               echo $ZB_PREFIX | tr -d '\n';\
               echo ${SNAPARR[1]/$STAMP/} | tr -d '\n';\
               echo ":" | tr -d '\n';\
               echo ${STAMP/-/})"
    echo $NEWSNAP
}

# Separate out the snapshots - eventually will need to rename the ZA snaps and
# remove the ZR ones
for SNAP in $SNAPLIST
do
    if [[ $SNAP == *"$ZA_PREFIX"* ]]
    then
        echo "Adding $SNAP to rename list"
        ZA_SNAPS+=($SNAP)
    elif [[ $SNAP == *"$ZR_PREFIX"* ]]
    then
        echo "Adding $SNAP to delete list"
        ZR_SNAPS+=($SNAP)
    fi
done

for ZA_SNAP in "${ZA_SNAPS}"
do
    RN_SNAP=$(rename_snap $ZA_SNAP)
    echo "Renaming $ZA_SNAP to $RN_SNAP"
done
