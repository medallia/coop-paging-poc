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
#include <libaio.h>

#define COLUMNS 600
#define SEGS 10
#define L 65536
#define SIZE (sizeof(int) * L)

using namespace std;

static int *filebase = NULL;
static int *rebase = NULL;
static int segnum = -1;
static vector<int *> unused;
static int fd;

static void init() {
    fd = open("/bigfile", O_TRUNC | O_RDWR | O_NONBLOCK | O_DIRECT);
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

static int *getint() {
    if (unused.empty())
        return new int[L];
    int *p = unused.back();
    unused.pop_back();
    return p;
}

struct Segment {
    set<int> hot;
    int *columns[COLUMNS];
    off_t pos[COLUMNS];
    bool mapped;
    io_context_t ctxp;
    int pending;

    Segment() {
        mapped = true;
        pending = 0;

        memset(&ctxp, 0, sizeof(ctxp));
        int res = io_setup(COLUMNS, &ctxp);
        if (res != 0) {
            errno = -res;
            perror("io_setup");
        }

        ++segnum;

        printf("Initializing segment %d... %p\n", segnum, NULL);

        for(int i = 0; i < COLUMNS; ++i) {
            pos[i] = COLUMNS*L*segnum + i*L;
            columns[i] = & filebase[pos[i]];

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
        if (mapped) {

            for(const int oldseg : hot) {
                if (newhot.count(oldseg) != 0)
                    continue;
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
            }
        } else {
            vector<int> toread;
            for(const int col : hot) {
                if (newhot.count(col) != 0)
                    continue;
                unused.push_back(columns[col]);
                columns[col] = NULL;
            }
            for(const int col : newhot) {
                if (hot.count(col) != 0)
                    continue;
                columns[col] = getint();
                toread.push_back(col);
            }
            const int n = toread.size();
            pending = n;
            if (n != 0) {
                int idx = 0;
                iocb ios[n];
                iocb *iosp[n];
                for(const int col : toread) {
                    io_prep_pread(& ios[idx], fd, columns[col], SIZE, pos[col] * sizeof(int));
                    ios[idx].data = NULL;
                    iosp[idx] = & ios[idx];
                    ++idx;
                }
                int res = io_submit(ctxp, n, iosp);
                if (res != n) {
                    printf("Got back %d\n", res);
                    errno = -res;
                    perror("io_submit");
                }
            }
        }

        hot = std::set<int>(newhot);
    }

    int peek(const set<int> &cols) {
        int res = 0;
        for(const int &col : cols) {
            int *c = columns[col];
            for(int i = 0; i < L; i+=1024)
                res += c[i];
        }
        return res;
    }

    int calc(const set<int> &cols) {
        if (pending != 0) {
            struct io_event events[pending];
            int res = io_getevents(ctxp, pending, pending, events, NULL);
            if (res != pending) {
                printf("got back %d\n", res);
                perror("io_getevents");
            }
            pending = 0;
        }
        int res = 0;
        for(const int &col : cols) {
            int *c = columns[col];
            for(int j = 0; j < 32; ++j)
                for(int i = 0; i < L; ++i)
                    res += c[i] + j;
        }
        return res;
    }

    void unmap() {
        for(int i = 0; i < COLUMNS; ++i)
            columns[i] = NULL;
        mapped = false;
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
        f[1] = std::async([]() {
            return 0;
        });
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

static void phaseshift(Segment *s) {
    if (munmap(filebase, SIZE * COLUMNS * SEGS) != 0) {
        perror("munmap");
    }
    for(int i = 0; i < SEGS; ++i)
        s[i].unmap();
}

int main(int argc, char **argv) {
    init();

    Segment s[SEGS];
    string d;

    printf("Done Init\n");

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

        phaseshift(s);

        sethot(s, hot);
        benchmark("Calc F1", s, hot);
        benchmark("Calc F2", s, hot);

        hot.insert(COLUMNS * 3 / 4);
        hot.insert(0);
        sethot(s, hot);
        benchmark("Calc G1", s, hot);
        benchmark("Calc G2", s, hot);

    }
}
