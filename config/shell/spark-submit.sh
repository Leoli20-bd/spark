#!/bin/bash

spark-sql -f common.sql \
--master local[2] \
--name test-sql \
> std.log