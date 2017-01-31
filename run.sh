#! /usr/bin/sh

# run using `sh run.sh`

PYTHONPATH='.'
luigi --module test1 Mortality --local-scheduler