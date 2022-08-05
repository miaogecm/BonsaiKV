#!/bin/bash

NR_DIMM=12
FILES=(
  "bonsai:1"
  "log:6"
  "pnopool:9"
  "pvalpool:98"
)

ndctl destroy-namespace all --force

echo > .mapping

for i in $(seq 0 $((NR_DIMM - 1))); do
  rm -rf /mnt/ext4/dimm$i
  for file in "${FILES[@]}"; do
    IFS=':' read -ra info <<< $file
    filename=${file[0]}
    size=$((${file[1]} * 1024 * 1024 * 1024))
    echo "creating /mnt/ext4/dimm$i/$filename ($size bytes)"
    output=$(ndctl create-namespace --size=$size --region=region$i --mode=devdax | tee /dev/tty)
    dev=$(echo "$output" | python3 -c "import sys, json; print(json.load(sys.stdin)['daxregion']['devices'][0]['chardev'])")
    echo "/mnt/ext4/dimm$i/$filename -> /dev/$dev" >> .mapping
    ln -s /dev/$dev /mnt/ext4/dimm$i
  done
done

echo "devdax setup OK"
