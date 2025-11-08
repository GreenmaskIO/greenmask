#!/bin/bash

while read line
do
   printf "%s" "$line" | md5sum | awk '{print $1}'
done