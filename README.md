# Requirement:

1. In each server, there is multiple switches running.  Each switch instance has one directory that store the cdr.
2. The directory path is specified in a conf file
3. There are two different set of switches, one is v3, one is v5.  Each version has different conf file and cdr path.
4. You can loop up the path by :
  a. Check the running `ps –aef |grep dnl_softswitch` 
  b. Check the path for config file
  c. Check the **cdr** path
5. There is 1 file created each minute.
6. For each new file, you will extract the needed files ( provided in the CDR dictionary) and import it to Google Big Data
7. It would be better if you can make the field selection configurable and changeable in the future.
8. You should have different table per switch , named with IP+public IP
9. Each switch can be listened on one or more of 5060 ports.  Each IP+5060 port must belong to just one switch.
10. You will need to setup the indexes for fast search.  The fields we nee to enable searches are :
..a. Duration is Zero or Non Zero
..b. Time 
..c. Origination_trunk
..d. Terminatino_trunk
..e. Orig ANI
..f. Orig DNIS
..g. Term ANI 
..h. Term DNIS
..i. Country
..j. Code Name
..k. Code
11. The group by can be:
a. Origination_trunk
b. Terminatino_trunk
c. Origination Host
d. Termination host
e. Date
f. Country
g. Code Name
h. Code

When retrieval, we can search based on:
a. Duration is Zero or Non Zero
b. Time 
c. Origination_trunk
d. Terminatino_trunk
e. Orig ANI
f. Orig DNIS
g. Term ANI 
h. Term DNIS
i. Country
j. Code Name
k. Code

The http request should include a “-compress” field to specify whether to return raw csv or tar.gz file.
The request should immediately return an unique request ID
Then another request is to ping the reueset ID to ping the status ( waiting, in progress, finished ).  If finished, the result file should be returned.
There should be a secret key associated with each IP’s.
The request must provide the secrect key to query CDR of an ip.
