# kill master and worker jobs
for i in `ps aux | grep '../mrmaster' | awk '{ print $2 }'`; do kill $i ; done
for i in `ps aux | grep '../mrworker' | awk '{ print $2 }'`; do kill $i ; done
