#!/bin/bash
begin=`date +%s`
for((i=0;i<=100;i++))
do
   touch "file${i}.txt"
   echo "hello" > "file${i}.txt"
done
end=`date +%s`

echo "test finished in `expr $end - $begin` seconds for 100 files with 1 node"
echo "test finished in `expr $end - $begin` seconds for 100 files with 1 node" >> testResults.txt

sleep 3

begin2=`date +%s`

cat /home/ioana/Desktop/dummy2.txt > dummyfile
end2=`date +%s`
echo "test finished in `expr $end2 - $begin2` seconds for 1 file 1000 Kb with 1 node"
echo "test finished in `expr $end2 - $begin2` seconds for 1 file 1000 Kb with 1 node" >> testResults.txt

rm file*
rm dummyfile