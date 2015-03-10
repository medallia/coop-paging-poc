#! /bin/bash

echo $$ > /sys/fs/cgroup/memory/memtest/tasks
exec ./memtest
