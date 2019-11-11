#!/bin/sh

spark-submit \
--class predicad.Main \
--master local[4] \
"$(dirname $0)"/target/scala-2.11/predicad_2.11-0.1.jar $*