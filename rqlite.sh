#!/bin/bash
declare -i node=$1
declare -i port=$2

cd $HOME
id_file=`find . -name rqlite-v5.10.2-linux-amd64`
cd $id_file || exit

if [ $node -eq 1 ]
then
  ./rqlited -node-id 1 ~/node.1
fi

if [ $node -gt 1 ]
then
  port2=`expr $port + 1`
  ./rqlited -node-id $node -http-addr localhost:$port -raft-addr localhost:$port2 -join http://localhost:4001 ~/node.$1&
fi

