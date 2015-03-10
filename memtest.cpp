#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <set>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <vector>
#include <functional>
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <chrono>
#include <ctime>
#include <stdint.h>
#include <random>
#include <cassert>
#include <thread>
#include <future>

#define COLUMNS 600
#define SEGS 20
#define L 65536
#define SIZE (sizeof(int) * L)

using namespace std;

int *filebase = NULL;
int *rebase = NULL;

int segnum = -1;

static void init() {
    int fd = open("/bigfile", O_TRUNC | O_RDWR);
    if (fd < 0) {
        perror("open");
    }
    ftruncate(fd, SIZE * COLUMNS * SEGS);

    filebase = (int *) mmap(NULL, SIZE * COLUMNS * SEGS, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (filebase == (int *)-1) {
        perror("mmap");
    }
    if (madvise(filebase, SIZE * COLUMNS * SEGS, MADV_SEQUENTIAL) != 0) {
        perror("madvise seq");
    }
    rebase = filebase;
}

static void post() {
    if (madvise(filebase, SIZE * COLUMNS * SEGS, MADV_REMOVE) != 0) {
        perror("madvise punch");
    }
}

struct Segment {
    set<int> hot;
    int *columns[COLUMNS];
    Segment() {

        ++segnum;

        printf("Initializing segment %d... %p\n", segnum, NULL);

        for(int i = 0; i < COLUMNS; ++i) {
            columns[i] = & filebase[COLUMNS*L*segnum + i * L];

            for(int j = 0; j < L; ++j)
                columns[i][j] = i*j;

            if (msync(columns[i], SIZE, MS_SYNC) != 0) {
                perror("msync");
            }
            if (madvise(columns[i], SIZE, MADV_DONTNEED) != 0) {
                perror("madvise noneed");
            }

        }
    }

    void sethot(const set<int> &newhot) {
        for(const int oldseg : hot) {
            if (newhot.count(oldseg) != 0)
                continue;
//            if (munlock(columns[oldseg], SIZE) != 0) {
//                perror("munlock");
//            }
            if (madvise(columns[oldseg], SIZE, MADV_DONTNEED) != 0) {
                perror("madvise notneed");
            }
        }
        for(const int seg : newhot) {
            if (hot.count(seg) != 0)
                continue;
            if (madvise(columns[seg], SIZE, MADV_WILLNEED) != 0) {
                perror("madvise need");
            }
//            if (mlock(columns[seg], SIZE) != 0) {
//                perror("mlock");
//            }
        }
        hot = std::set<int>(newhot);
    }

    int peek(const set<int> &cols) {
        int res = 0;
        for(const int &col : cols) {
            int *c = columns[col];
            for(int i = 0; i < SIZE; i+=1024)
                res += c[i];
        }
        return res;
    }

    int calc(const set<int> &cols) {
        int res = 0;
        for(const int &col : cols) {
            int *c = columns[col];
            for(int j = 0; j < 1; ++j)
                for(int i = 0; i < SIZE; ++i)
                    res += c[i];
        }
        return res;
    }

};

static void benchmark(const std::string &str, Segment *s, const set<int> &cols, bool peekaboo = false) {
    auto start = std::chrono::system_clock::now();

    std::future<int> f[2];

    if (peekaboo) {
        f[1] = std::async( std::launch::async, [=]() {
        int res = 0;
        for(int snum = 0; snum < SEGS; ++snum)
            res += s[snum].peek(cols);
        return res;
        });
    } else {
        f[1] = std::async([]() { return 0; });
    }

    f[0] = std::async( std::launch::async, [=]() {
        int res = 0;
        for(int snum = 0; snum < SEGS; ++snum)
            res += s[snum].calc(cols);
        return res;
    });
    
    int res = f[0].get();
    f[1].get();

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;

    cout << str << ": " << res << " " << elapsed_seconds.count() << endl;

}

static void sethot(Segment *s, const set<int> &cols) {
    auto start = std::chrono::system_clock::now();

    for(int i=0; i<SEGS; ++i)
        s[i].sethot(cols);

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;

    cout << "SetHot " << elapsed_seconds.count() << endl;
}

int main(int argc, char **argv) {
  if (mlockall(MCL_CURRENT) != 0) {
    perror("mlockall");
  }

    init();


    Segment s[SEGS];
    string d;


    printf("Done Init\n");

    int res = 0;

    {
        set<int> hot;

        sethot(s, hot);

        for(int j = COLUMNS*2/4; j < COLUMNS; ++j)
            hot.insert(j);
        benchmark("Calc II", s, hot);

        hot.clear();
        for(int j = 0; j < COLUMNS/4; ++j)
            hot.insert(j);


        sethot(s, hot);
        printf("Done warming\n");
        benchmark("Calc A1", s, hot);
        benchmark("Calc A2", s, hot);

        hot.clear();
        sethot(s, hot);
        for(int j = COLUMNS/4; j < COLUMNS/2; ++j)
            hot.insert(j);

        benchmark("Calc B1", s, hot);
        benchmark("Calc B2", s, hot);

        hot.clear();
        for(int j = COLUMNS*2/4; j < COLUMNS*3/4; ++j)
            hot.insert(j);

        benchmark("Calc C1", s, hot, true);
        benchmark("Calc C2", s, hot, true);

        hot.insert(COLUMNS*3/4 + 2);
        sethot(s, hot);
        sethot(s, hot);
        benchmark("Calc D1", s, hot);

        hot.erase(COLUMNS*3/4 + 2);
        hot.insert(COLUMNS*3/4 + 3);
        benchmark("Calc D2", s, hot, true);

        hot.erase(COLUMNS*3/4 + 3);
        hot.insert(COLUMNS*3/4 + 4);
        benchmark("Calc D3", s, hot, false);
        benchmark("Calc D4", s, hot, false);

        hot.clear();
        sethot(s, hot);

        for(int j = 0; j < 5; ++j) {
            hot.clear();
            int block = j % 4;
            for(int i = (COLUMNS * block) / 4; i < (COLUMNS * (block+1)) / 4; ++i)
                hot.insert(i);
            benchmark("Calc E", s, hot);
        }
    }

    printf("Done Hot: %d\n", res);

    post();

}
