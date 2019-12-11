#!/bin/bash

# Temporary migration script to transfer from zrep + zfs-auto-snapshot to zback,
# needs to be run on primary and secondary

DATASET=$1
ZA_PREFIX="zfs-auto-snap_"
ZR_PREFIX="zrep_"
ZB_PREFIX="zback:"

RM_PROPS=("com.sun:auto-snapshot" \
          "com.sun:auto-snapshot:hourly" \
          "com.sun:auto-snapshot:daily" \
          "com.sun:auto-snapshot:weekly" \
          "com.sun:auto-snapshot:monthly" \
          "zrep:lock-time" \
          "zrep:lock-pid" \
          "zrep:src-fs" \
          "zrep:dest-fs" \
          "zrep:src-host" \
          "zrep:dest-host" \
          "zrep:savecount" \
          "zrep:master")

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

sort_snapshots()
{
    # Separate out the snapshots - eventually will need to rename the ZA snaps and
    # remove the ZR ones
    SNAPLIST=$(zfs list -t snapshot -o name -r $DATASET)
    ZA_SNAPS=()
    ZR_SNAPS=()
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
    
    for ZA_SNAP in "${ZA_SNAPS[@]}"
    do
        RN_SNAP=$(rename_snap $ZA_SNAP)
        echo "Renaming $ZA_SNAP to $RN_SNAP"
        zfs rename $ZA_SNAP $RN_SNAP
    done
    
    for ZR_SNAP in "${ZR_SNAPS[@]}"
    do
        echo "Removing zrep snapshot $ZR_SNAP"
        zfs destroy $ZR_SNAP
    done
}

sort_props()
{
    for PROP in "${RM_PROPS[@]}"
    do
        zfs inherit $PROP $DATASET
    done
    zfs set zback:manage=yes $DATASET
}

sort_props
sort_snapshots
