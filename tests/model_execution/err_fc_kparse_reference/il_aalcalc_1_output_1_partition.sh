#!/bin/bash
SCRIPT=$(readlink -f "$0") && cd $(dirname "$SCRIPT")

# --- Script Init ---

set -e
set -o pipefail
mkdir -p log
rm -R -f log/*

error_handler(){
   echo 'Run Error - terminating'
   exit_code=$?
   set +x
   group_pid=$(ps -p $$ -o pgid --no-headers)
   sess_pid=$(ps -p $$ -o sess --no-headers)
   printf "Script PID:%d, GPID:%s, SPID:%d" $$ $group_pid $sess_pid >> log/killout.txt

   if hash pstree 2>/dev/null; then
       pstree -pn $$ >> log/killout.txt
       PIDS_KILL=$(pstree -pn $$ | grep -o "([[:digit:]]*)" | grep -o "[[:digit:]]*")
       kill -9 $(echo "$PIDS_KILL" | grep -v $group_pid | grep -v $$) 2>/dev/null
   else
       ps f -g $sess_pid > log/subprocess_list
       PIDS_KILL=$(pgrep -a --pgroup $group_pid | grep -v celery | grep -v $group_pid | grep -v $$)
       echo "$PIDS_KILL" >> log/killout.txt
       kill -9 $(echo "$PIDS_KILL" | awk 'BEGIN { FS = "[ \t\n]+" }{ print $1 }') 2>/dev/null
   fi
   exit $(( 1 > $exit_code ? 1 : $exit_code ))
}
trap error_handler QUIT HUP INT KILL TERM ERR

touch log/stderror.err
ktools_monitor.sh $$ & pid0=$!

# --- Setup run dirs ---

find output/* ! -name '*summary-info*' -type f -exec rm -f {} +
mkdir output/full_correlation/

rm -R -f fifo/*
mkdir fifo/full_correlation/
rm -R -f work/*
mkdir work/kat/
mkdir work/full_correlation/
mkdir work/full_correlation/kat/
mkfifo fifo/il_P1
mkfifo fifo/il_S1_summary_P1

mkdir work/il_S1_summaryaalcalc
mkfifo fifo/full_correlation/il_S1_summary_P1

mkdir work/full_correlation/il_S1_summaryaalcalc

# --- Do insured loss computes ---


tee < fifo/il_S1_summary_P1 work/il_S1_summaryaalcalc/P1.bin > /dev/null & pid1=$!

( summarycalc -f  -1 fifo/il_S1_summary_P1 < fifo/il_P1 ) 2>> log/stderror.err  &

( eve 1 1 | getmodel | gulcalc -S100 -L100 -r -j fifo/full_correlation/gul_P1 -a1 -i - | fmcalc -a2 > fifo/il_P1  ) 2>> log/stderror.err &

wait $pid1

# --- Do computes for fully correlated output ---

( fmcalc-a2 < fifo/full_correlation/gul_P1 > fifo/full_correlation/il_P1 ) 2>> log/stderror.err & fcpid1=$!

wait $fcpid1


# --- Do insured loss computes ---


tee < fifo/full_correlation/il_S1_summary_P1 work/full_correlation/il_S1_summaryaalcalc/P1.bin > /dev/null & pid1=$!

( summarycalc -f  -1 fifo/full_correlation/il_S1_summary_P1 < fifo/full_correlation/il_P1 ) 2>> log/stderror.err  &

wait $pid1


# --- Do insured loss kats ---


# --- Do insured loss kats for fully correlated output ---


aalcalc -Kil_S1_summaryaalcalc > output/il_S1_aalcalc.csv & lpid1=$!
aalcalc -Kfull_correlation/il_S1_summaryaalcalc > output/full_correlation/il_S1_aalcalc.csv & lpid2=$!
wait $lpid1 $lpid2

rm -R -f work/*
rm -R -f fifo/*

# Stop ktools watcher
kill -9 $pid0
