# Requirement:

1. In each server, there is multiple switches running.  Each switch instance has one directory that store the cdr.
2. The directory path is specified in a conf file
3. There are two different set of switches, one is v3, one is v5.  Each version has different conf file and cdr path.
4. You can loop up the path by :
    1. Check the running `ps –aef |grep dnl_softswitch` 
    2. Check the path for config file
    3. Check the **cdr** path
5. There is 1 file created each minute.
6. For each new file, you will extract the needed files ( provided in the CDR dictionary) and import it to Google Big Data
7. It would be better if you can make the field selection configurable and changeable in the future.
8. You should have different table per switch , named with IP+public IP
9. Each switch can be listened on one or more of 5060 ports.  Each IP+5060 port must belong to just one switch.
10. You will need to setup the indexes for fast search.  The fields we nee to enable searches are :
    1. Duration is Zero or Non Zero
    2. Time 
    3. Origination_trunk
    4. Terminatino_trunk
    5. Orig ANI
    6. Orig DNIS
    7. Term ANI 
    8. Term DNIS
    9. Country
    10. Code Name
    11. Code
11. The group by can be:
    1. Origination_trunk
    2. Terminatino_trunk
    3. Origination Host
    4. Termination host
    5. Date
    6. Country
    7. Code Name
    8. Code

When retrieval, we can search based on:

1. Duration is Zero or Non Zero
2. Time 
3. Origination_trunk
4. Terminatino_trunk
5. Orig ANI
6. Orig DNIS
7. Term ANI 
8. Term DNIS
9. Country
10. Code Name
11. Code

The http request should include a **-compress** field to specify whether to return *raw*, *csv* or *tar.gz* file.
The request should immediately return an unique request ID
Then another request is to ping the reueset ID to ping the status ( waiting, in progress, finished ).  If finished, the result file should be returned.
There should be a secret key associated with each IP’s.
The request must provide the secrect key to query CDR of an ip.