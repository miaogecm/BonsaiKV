#!/bin/bash

ps H -C $1 -o 'pid tid psr args comm'
