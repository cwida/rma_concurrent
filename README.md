---
Introduction
---

This is the source code of the implementation and of the experiments featured in the paper ''D. De Leo, P. Boncz, Fast Concurrent Reads and Updates with PMAs, GRADES 2019'' [1]. It consists of a single program, `pmacomp`, inclusive of the data structures tested and the code to run the simulations. Since the program makes use of a few O.S. dependent constructs (`libnuma`, `memfd_create`), it only supports Linux.

The driver `pmacomp` and the developed data structures are licensed under the GPL v3 terms. Still, the source code contains some third-party data structures, included for comparison purposes, that may be licensed according to different terms.


---
Build
---

Requirements:
* Linux kernel v4.17+
* Autotools, [Autoconf 2.69+](https://www.gnu.org/software/autoconf/)
* A C++17 compliant compiler. We tested and executed the program with Clang 7.
* libnuma 2.0+
* [libpapi 5.5+](http://icl.utk.edu/papi/)
* [libevent 2.1.1+](http://www.libevent.org)
* As in [3], memory rewiring is performed on [huge pages](https://www.kernel.org/doc/Documentation/vm/hugetlbpage.txt). Its support may need to be explicitly enabled by a privileged user. In our environment, we set:
```bash
echo 4294967296 > /proc/sys/vm/nr_overcommit_hugepages
echo 1 > /proc/sys/vm/overcommit_memory
```

To compile the whole suite of experiments use:
```
autoreconf -iv
mkdir build && cd build
../configure --enable-optimize --disable-debug --with-masstree --with-tcmalloc
make -j
```

There is no "install" target. You can optionally execute the included test suite with `make check` (this may take up to 30 minutes to complete...).

---
Running the driver
---

The driver is derived from [2], it follows the same structure and general arguments:
```
./pmacomp -e <experiment_name> [experiment_params] -d <distribution> [distribution_params] -a <data_structure> [data_structure_params] [num_threads] -v
```

An execution of the program accounts for a single run of the experiment.  Each experiment either creates or appends, if it already exists, the outcomes of the simulation into the SQLite3 database `results.sqlite3`. The database will contain:
* A table `executions` with a global unique id, to join the other tables in a star schema, and the name of the experiment,
* A table `parameters` with all explicit and default parameters set and,
* An additional table, whose name depends on the experiment, with the actual results of the simulation. 

For the paper, all simulations have been repeated 5 times. Each run was executed with different seeds for the random generators.


##### Experiments

Each experiment stores the outcomes in a table with the same name in the database `results.sqlite3`. For the description of the command line arguments associated to each experiment, check `./pmacomp -h`. In the paper, the following experiments were used:

<table width="100%">
    <tr>
        <th>Name</th>
        <th>Description</th>
        <th>Data stored in results.sqlite3</th>
    </tr>
    <tr>
        <td><tt>parallel_insert</tt></td>
        <td>Starting from an empty data structure, perform <tt>I</tt> insertions using <tt>thread_inserts</tt> threads, while, at the same time, <tt>thread_scans</tt> threads sequentially scan all elements contained in the data structure. The simulation ends once all insertions have been completed.</td>
        <td>The attribute <tt>time_insert</tt> reports the completion time, in microseconds, while <tt>num_elements_scan</tt> the cumulative number of elements visited by the <tt>thread_scans</tt> threads.</td>
    </tr>
    <tr>
        <td><tt>parallel_idls</tt></td>
        <td>First, insert <tt>initial_size</tt> elements. Eventually, repeatedly perform <tt>idls_group_size</tt> consecutive insertions followed by                <tt>idls_group_size</tt> consecutive deletions of existing elements. The simulation ends after <tt>num_inserts</tt> operations (insertions/deletions), after the initial insertions, have been performed. Use <tt>thread_inserts</tt> threads for the updates, while at the same time, <tt>thread_scans</tt> independent threads sequentially scan all elements contained in the data structure.</td>
        <td>The attribute <tt>t_updates_millisecs</tt> reports, in milliseconds, the completion time to perform <tt>num_inserts</tt> update operations (insertions and deletions) with <tt>thread_inserts</tt>. The attribute <tt>scan_updates</tt> states the cumulative number of elements visited by the <tt>thread_scans</tt> threads.</td>
    </tr>
</table>

For convenience, a few view definitions are provided [here](views.sql) to help visualising the relevant data. \
The notebook for Mathematica 11.3 used to create the plots in the paper is available [here](https://github.com/whatsthecraic/pma_mathematica_notebooks/blob/master/parallel.nb).

##### Distributions

For the paper, the following distributions were considered:

* `uniform`: the keys are a random permutation of the interval [1, total elements to insert].
* `uniform --beta β`: the most significant 4 bytes of the keys are extracted from the uniform distribution of range [1, β]. The 4 least significant bytes are created using a unique counter.
* `zipf --alpha α --beta β`: the most significant 4 bytes of the keys are generated from a random permutation of the outcomes of the Zipfian distribution of parameter α and range β. The 4 least significant bytes are created using a unique counter.
* `apma_sequential`: sequential pattern, with the keys generated as 1, 2, 3, ...

---
Repeating the experiments
---
The following are the listings to repeat the experiments presented in [1].


##### Average throughput

Figure 3a, only insertions with 16 threads:
```bash
# MassTree [4], source code: third-party/masstree/*, data_structures/masstree/* (wrapper)
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a masstree_par --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a masstree_par --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a masstree_par --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a masstree_par --thread_inserts 16 -v
# BwTree [5], source code: third-party/openbwtree/*, data_structures/bwtree/* (wrapper)
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a openbwtree --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a openbwtree --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a openbwtree --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a openbwtree --thread_inserts 16 -v
# ART [6] / B+ Tree, source code: third-party/art-olc/*,  data_structures/abtree/parallel/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a abtree_parallel -l 256 --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 16 -v
# PMA, source code: data_structures/rma/batch_processing/*, data_structures/rma/common/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_batch -b 64 -l 128 --hugetlb --extent_size 1 --delay 100 --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 16 -v
```

Figure 3b, 12 threads dedicated to the insertions, 4 threads dedicated to the scans:
```bash
# MassTree [4], source code: third-party/masstree/*, data_structures/masstree/* (wrapper)
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a masstree_par --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a masstree_par --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a masstree_par --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a masstree_par --thread_inserts 12 --thread_scans 4 -v
# BwTree [5], source code: third-party/openbwtree/*, data_structures/bwtree/* (wrapper)
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a openbwtree --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a openbwtree --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a openbwtree --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a openbwtree --thread_inserts 12 --thread_scans 4 -v
# ART [6] / B+ Tree, source code: third-party/art-olc/*,  data_structures/abtree/parallel/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a abtree_parallel -l 256 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 12 --thread_scans 4 -v
# PMA, source code: data_structures/rma/batch_processing/*, data_structures/rma/common/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_batch -b 64 -l 128 --hugetlb --extent_size 1 --delay 100 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 12 --thread_scans 4 -v
```

Figure 3c, 8 threads dedicated to the insertions, 8 threads dedicated to the scans:
```bash
# MassTree [4], source code: third-party/masstree/*, data_structures/masstree/* (wrapper)
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a masstree_par --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a masstree_par --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a masstree_par --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a masstree_par --thread_inserts 8 --thread_scans 8 -v
# BwTree [5], source code: third-party/openbwtree/*, data_structures/bwtree/* (wrapper)
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a openbwtree --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a openbwtree --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a openbwtree --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a openbwtree --thread_inserts 8 --thread_scans 8 -v
# ART [6] / B+ Tree, source code: third-party/art-olc/*,  data_structures/abtree/parallel/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a abtree_parallel -l 256 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 8 --thread_scans 8 -v
# PMA, source code: data_structures/rma/batch_processing/*, data_structures/rma/common/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_batch -b 64 -l 128 --hugetlb --extent_size 1 --delay 100 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 8 --thread_scans 8 -v
```

Figure 3d, only updates (insertions/deletions) with 16 threads:
```bash
# MassTree [4], source code: third-party/masstree/*, data_structures/masstree/* (wrapper)
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a masstree_par --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a masstree_par --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a masstree_par --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a masstree_par --thread_inserts 16 -v
# BwTree [5], source code: third-party/openbwtree/*, data_structures/bwtree/* (wrapper)
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a openbwtree --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a openbwtree --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a openbwtree --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a openbwtree --thread_inserts 16 -v
# ART [6] / B+ Tree, source code: third-party/art-olc/*,  data_structures/abtree/parallel/*
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 16 -v
# PMA, source code: data_structures/rma/batch_processing/*, data_structures/rma/common/*
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a rma_batch -b 64 -l 128 --hugetlb --extent_size 1 --delay 100 --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 16 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 16 -v
```

Figure 3e, 12 threads dedicated to the updates (insertions and deletions), 4 threads dedicated to the scans:
```bash
# MassTree [4], source code: third-party/masstree/*, data_structures/masstree/* (wrapper)
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a masstree_par --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a masstree_par --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a masstree_par --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a masstree_par --thread_inserts 12 --thread_scans 4 -v
# BwTree [5], source code: third-party/openbwtree/*, data_structures/bwtree/* (wrapper)
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a openbwtree --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a openbwtree --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a openbwtree --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a openbwtree --thread_inserts 12 --thread_scans 4 -v
# ART [6] / B+ Tree, source code: third-party/art-olc/*,  data_structures/abtree/parallel/*
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 12 --thread_scans 4 -v
# PMA, source code: data_structures/rma/batch_processing/*, data_structures/rma/common/*
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a rma_batch -b 64 -l 128 --hugetlb --extent_size 1 --delay 100 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 12 --thread_scans 4 -v
```

Figure 3f, 8 threads dedicated to the updates (insertions and deletions), 8 threads dedicated to the scans:
```bash
# MassTree [4], source code: third-party/masstree/*, data_structures/masstree/* (wrapper)
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a masstree_par --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a masstree_par --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a masstree_par --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a masstree_par --thread_inserts 8 --thread_scans 8 -v
# BwTree [5], source code: third-party/openbwtree/*, data_structures/bwtree/* (wrapper)
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a openbwtree --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a openbwtree --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a openbwtree --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a openbwtree --thread_inserts 8 --thread_scans 8 -v
# ART [6] / B+ Tree, source code: third-party/art-olc/*,  data_structures/abtree/parallel/*
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a abtree_parallel -l 256 --thread_inserts 8 --thread_scans 8 -v
# PMA, source code: data_structures/rma/batch_processing/*, data_structures/rma/common/*
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d uniform --beta 134217728 -a rma_batch -b 64 -l 128 --hugetlb --extent_size 1 --delay 100 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 1.5 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_idls --initial_size 1073741824 -I 1073741824 --idls_group_size 16777216 -d zipf --alpha 2 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay 100 --thread_inserts 8 --thread_scans 8 -v
```

##### Asynchronous updates

Figure 4a, only insertions with 16 threads:
```bash
# PMA Baseline, source code: data_structures/rma/baseline/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_baseline -b 64 -l 128 --hugetlb --extent_size 1 --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_baseline -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_baseline -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_baseline -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 16 -v
# One by one, source code: data_structures/rma/one_by_one/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_1by1 -b 64 -l 128 --hugetlb --extent_size 1 --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_1by1 -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_1by1 -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 16 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_1by1 -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 16 -v
# Batch processing, source code: data_structures/rma/batch_processing/*
for d in 0 100 200 800; do # delay
    ./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_batch -b 64 -l 128 --hugetlb --extent_size 1 --delay ${d} --thread_inserts 16 -v
    ./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay ${d} --thread_inserts 16 -v
    ./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay ${d} --thread_inserts 16 -v
    ./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay ${d} --thread_inserts 16 -v
done
```

Figure 4b, 12 threads dedicated to the insertions, 4 threads dedicated to the scans:
```bash
# PMA Baseline, source code: data_structures/rma/baseline/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_baseline -b 64 -l 128 --hugetlb --extent_size 1 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_baseline -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_baseline -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_baseline -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 12 --thread_scans 4 -v
# One by one, source code: data_structures/rma/one_by_one/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_1by1 -b 64 -l 128 --hugetlb --extent_size 1 --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_1by1 -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_1by1 -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 12 --thread_scans 4 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_1by1 -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 12 --thread_scans 4 -v
# Batch processing, source code: data_structures/rma/batch_processing/*
for d in 0 100 200 800; do # delay
    ./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_batch -b 64 -l 128 --hugetlb --extent_size 1 --delay ${d} --thread_inserts 12 --thread_scans 4 -v
    ./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay ${d} --thread_inserts 12 --thread_scans 4 -v
    ./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay ${d} --thread_inserts 12 --thread_scans 4 -v
    ./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay ${d} --thread_inserts 12 --thread_scans 4 -v
done
```

Figure 4c, 8 threads dedicated to the insertions, 8 threads dedicated to the scans:
```bash
# PMA Baseline, source code: data_structures/rma/baseline/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_baseline -b 64 -l 128 --hugetlb --extent_size 1 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_baseline -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_baseline -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_baseline -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 8 --thread_scans 8 -v
# One by one, source code: data_structures/rma/one_by_one/*
./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_1by1 -b 64 -l 128 --hugetlb --extent_size 1 --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_1by1 -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_1by1 -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 8 --thread_scans 8 -v
./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_1by1 -b 64 -l 128 --extent_size 1 --hugetlb --thread_inserts 8 --thread_scans 8 -v
# Batch processing, source code: data_structures/rma/batch_processing/*
for d in 0 100 200 800; do # delay
    ./pmacomp -e parallel_insert -I 1073741824 -d uniform -a rma_batch -b 64 -l 128 --hugetlb --extent_size 1 --delay ${d} --thread_inserts 8 --thread_scans 8 -v
    ./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay ${d} --thread_inserts 8 --thread_scans 8 -v
    ./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 1.5 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay ${d} --thread_inserts 8 --thread_scans 8 -v
    ./pmacomp -e parallel_insert -I 1073741824 -d zipf --alpha 2 --beta 134217728 -a rma_batch -b 64 -l 128 --extent_size 1 --hugetlb --delay ${d} --thread_inserts 8 --thread_scans 8 -v
done
```

---
References
---
1. D. De Leo, P. Boncz. Fast concurrent reads and updates with PMAs. In GRADES 2019. [Paper](https://ir.cwi.nl/pub/28679). 
2. D. De Leo, P. Boncz. Packed Memory Arrays - Rewired. In ICDE 2019. [Paper](https://ir.cwi.nl/pub/28649), [Source code](https://github.com/cwida/rma).
3. F. Schuhknecht, J. Dittrich, A. Sharma. RUMA has it: rewired user-space memory access is possible! In VLDB 2016. [Paper](http://www.vldb.org/pvldb/vol9/p768-schuhknecht.pdf).
4. Y. Mao, E. Kohler†, R. Morris. Cache craftiness for fast multicore key-value storage. In EuroSys 2012. [Paper](https://pdos.csail.mit.edu/papers/masstree:eurosys12.pdf), [Source code](https://github.com/kohler/masstree-beta).
5. Z. Wang, A. Pavlo, H. Lim, V. Leis, H. Zhang, M. Kaminsky, and D. Andersen. Building a Bw-Tree takes more than just buzz words. In SIGMOD 2018. [Paper](https://hyeontaek.com/papers/openbwtree-sigmod2018.pdf), [Source code](https://github.com/wangziqi2016/index-microbench).
6. V. Leis, F. Scheibner, A. Kemper, T. Neumann. The ART of practical synchronization. In DAMON 2016. [Paper](https://db.in.tum.de/~leis/papers/artsync.pdf), [Source code](http://github.com/flode/ARTSynchronized).