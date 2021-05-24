#!/bin/bash
#declare -i node=$1
#declare -i port=$2
echo "NODE IS "
echo $1
echo $2
#cd $HOME
#pid_file=`find . -name rqlite-v5.10.2-linux-amd64`
#cd $pid_file || exit

cd "/home/ioana/Downloads/rqlite-v5.10.2-linux-amd64"
#./rqlited -node-id 1 ~/node.1

if [ $1 -eq 1 ]
then
  ./rqlited -node-id 1 ~/node.1
fi

if [ $1 -eq 2 ]
then
  ./rqlited -node-id $1 -http-addr localhost:$2 -raft-addr localhost:4004 -join http://localhost:4001 ~/node.2
  ./rqlited -node-id 2 -http-addr localhost:4003 -raft-addr localhost:4004 -join http://localhost:4001 ~/node.2
fi

if [ $1 -eq 3 ]
then
  ./rqlited -node-id $1 -http-addr localhost:$2 -raft-addr localhost:4006 -join http://localhost:4001 ~/node.2
fi


