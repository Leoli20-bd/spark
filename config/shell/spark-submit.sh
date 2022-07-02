#!/bin/bash

spark-sql -f /Users/haleli/myworld/project/java_pro/spark/config/sql/common.sql \
--master local[2] \
--name test-sql \
> std.log