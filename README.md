sstableconverter
===

# Description

sstableconverter converts sstables between Cassandra partitioner, normally between RandomPartitioner and Murmur3Partitioner. After converting, sstables can be loaded into the cluster via sstableloader distributed in Cassandra package.

# Version

* This version converts sstables from C* 2.0.  Please switch to branch 1.2 for C* 1.2 series.

# Requirements

* Maven

# Installation

After maven is installed, execute:

    ./build.sh

The executable jar will be stored in the target directory.

# Usage

    java -jar sstableconverter-2.0.jar -h

    usage: sstableconverter [options] <src_dir> <dst_dir>
    --
    Converts the sstables found in the directory <src_dir> from a source
    partitioner (specified by -s option)to a destination partitioner
    (specified -d option), storing the converted sstables on <dst_dir>.
    If the keyspace and cf options are not provided, <src_dir> is used ascf
    name and the parent directory of <dir_path> is used as the keyspace name.
    --
    Options are:
     -c,--cf <columnfamily>    Column family of <src_dir> (default: <src_dir>
                           name)
     -d,--dest-partitioner     sets the destination partitioner (default:
                           Murmur3Partitioner)
        --debug                display stack traces
     -h,--help                 display this help message
     -k,--ks <keyspace>        Keyspace of <src_dir> (default: <src_dir>/../
                               name)
     -s,--source-partitioner   sets the source partitioner (default:
                           RandomPartitioner)
     -v,--verbose              verbose output
