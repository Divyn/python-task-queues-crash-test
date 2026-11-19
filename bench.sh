set -e
ulimit -n 65535
cd /root/exp
/root/v/bin/python tq_race.py > results.csv
