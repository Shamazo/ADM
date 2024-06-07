#!/bin/bash

# Function to display help message
function display_help() {
  echo "Usage: source $0"
  echo "Options:"
  echo "-h, --help    Show help"
  echo ""
  echo "This script sets up a pair of named pipes for controlling collection by perf and sets the PERF_CTL_FD and"
  echo "PERF_CTL_ACK_FD environment variables to the file descriptors of the pipes. You can then use perf record/stat"
  echo "like so \`perf record --delay=-1 --control fd:\${PERF_CTL_FD},\${PERF_CTL_ACK_FD} -- {your binary here}\`."
  echo "With --delay=-1 meaning start without collecting events. In Proteus, profiling::pause/profiling::resume will"
  echo "control perf collection, but proteus must have the same PERF_CTL_FD and PERF_CTL_ACK_FD set in its env."
  echo "see man perf record and specifically the --control= option"
  exit 1
}

# Check if script is called with -h or --help
if [[ $1 == "-h" ]] || [[ $1 == "--help" ]]; then
  display_help
fi
ctl_fifo=/tmp/perf_ctl_fd.fifo
ack_fifo=/tmp/perf_ctl_fd_ack.fifo
test -p ${ctl_fifo} && unlink ${ctl_fifo}
test -p ${ack_fifo} && unlink ${ack_fifo}
mkfifo ${ctl_fifo}
exec {ctl_fd}<>${ctl_fifo}
mkfifo ${ack_fifo}
exec {ack_fd}<>${ack_fifo}
export PERF_CTL_FD=$ctl_fd
export PERF_CTL_ACK_FD=$ack_fd
echo "Set: PERF_CTL_FD=${PERF_CTL_FD} and PERF_CTL_ACK_FD=${PERF_CTL_ACK_FD}"