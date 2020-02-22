# kill master and worker jobs
for i in `ps aux | grep '../mrmaster' | grep -v grep | awk '{ print $2 }'`; do kill $i ; done
for i in `ps aux | grep '../mrworker' | grep -v grep | awk '{ print $2 }'`; do kill $i ; done
