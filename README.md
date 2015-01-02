py_dht_chord
============

An in-memory distributed hash table in python over the Chord protocol.

The goal of this project is to exercise a few concepts such as distributed hash-tables, consistent hashing and RPCs using Thrift.

It's based on the [Stoica et al. paper](http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=1180543&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D1180543) and on this [course assignment](https://seattle.poly.edu/wiki/EducationalAssignments/Chord).


TODO:
- Node leaves
- Node fails 
- Moving keys after a node join
- Retry after failed connections (stabilization may be running, keys may be still moving, network fail)
- Replication of data


