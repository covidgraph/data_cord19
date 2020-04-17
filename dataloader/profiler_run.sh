#/bin/bash
# it make sense to switch to one cpu-core only for profiling. 
#reqs:
# apt install kcachegrind
# pip3 install pyprof2calltree
export ENV=PROFILING
echo ${ENV}
python3 -m cProfile -o /tmp/pmp_profile.cprof ./main.py
pyprof2calltree -k -i /tmp/pmp_profile.cprof
