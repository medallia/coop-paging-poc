# coop-paging-poc
Proof-of-Concept for Cooperative Paging

# Setup
This needs basic setup to work, specifically a cgroup to restrict memory

```bash
sudo -i
cd /sys/fs/cgroup/memory/
mkdir memtest
cd memtest
echo 1000000000 > memory.limit_in_bytes
echo 100 > memory.swappiness 
echo 1 > memory.oom_control 
```

# Execute

To run, do
```bash
g++ -std=c++11 -O3 -march=native memtest.cpp -o memtest -lpthread && sudo bash ./go.sh
```

sudo is needed to enter the cgroup and to run mlockall
