#!/bin/bash

# ./gen_workload.sh <workload> <load_count> <op_count> <distrib>

WORKLOAD=$1
LOAD_COUNT=$2
OP_COUNT=$3
DISTRIB=$4

cat ./workload_spec/$WORKLOAD | sed -e "s|\${LOAD_COUNT}|$LOAD_COUNT|g" \
                                    -e "s|\${OP_COUNT}|$OP_COUNT|g" \
                                    -e "s|\${DISTRIB}|$DISTRIB|g" > .current_spec
./_gen_workload.sh $(pwd)/.current_spec
