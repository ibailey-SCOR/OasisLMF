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

rm -R -f fifo/*
rm -R -f work/*
mkdir work/kat/
mkfifo fifo/il_P1
mkfifo fifo/il_S1_summary_P1
mkfifo fifo/il_S1_summarypltcalc_P1
mkfifo fifo/il_S1_pltcalc_P1

mkfifo fifo/il_P2
mkfifo fifo/il_S1_summary_P2
mkfifo fifo/il_S1_summarypltcalc_P2
mkfifo fifo/il_S1_pltcalc_P2

mkfifo fifo/il_P3
mkfifo fifo/il_S1_summary_P3
mkfifo fifo/il_S1_summarypltcalc_P3
mkfifo fifo/il_S1_pltcalc_P3

mkfifo fifo/il_P4
mkfifo fifo/il_S1_summary_P4
mkfifo fifo/il_S1_summarypltcalc_P4
mkfifo fifo/il_S1_pltcalc_P4

mkfifo fifo/il_P5
mkfifo fifo/il_S1_summary_P5
mkfifo fifo/il_S1_summarypltcalc_P5
mkfifo fifo/il_S1_pltcalc_P5

mkfifo fifo/il_P6
mkfifo fifo/il_S1_summary_P6
mkfifo fifo/il_S1_summarypltcalc_P6
mkfifo fifo/il_S1_pltcalc_P6

mkfifo fifo/il_P7
mkfifo fifo/il_S1_summary_P7
mkfifo fifo/il_S1_summarypltcalc_P7
mkfifo fifo/il_S1_pltcalc_P7

mkfifo fifo/il_P8
mkfifo fifo/il_S1_summary_P8
mkfifo fifo/il_S1_summarypltcalc_P8
mkfifo fifo/il_S1_pltcalc_P8

mkfifo fifo/il_P9
mkfifo fifo/il_S1_summary_P9
mkfifo fifo/il_S1_summarypltcalc_P9
mkfifo fifo/il_S1_pltcalc_P9

mkfifo fifo/il_P10
mkfifo fifo/il_S1_summary_P10
mkfifo fifo/il_S1_summarypltcalc_P10
mkfifo fifo/il_S1_pltcalc_P10

mkfifo fifo/il_P11
mkfifo fifo/il_S1_summary_P11
mkfifo fifo/il_S1_summarypltcalc_P11
mkfifo fifo/il_S1_pltcalc_P11

mkfifo fifo/il_P12
mkfifo fifo/il_S1_summary_P12
mkfifo fifo/il_S1_summarypltcalc_P12
mkfifo fifo/il_S1_pltcalc_P12

mkfifo fifo/il_P13
mkfifo fifo/il_S1_summary_P13
mkfifo fifo/il_S1_summarypltcalc_P13
mkfifo fifo/il_S1_pltcalc_P13

mkfifo fifo/il_P14
mkfifo fifo/il_S1_summary_P14
mkfifo fifo/il_S1_summarypltcalc_P14
mkfifo fifo/il_S1_pltcalc_P14

mkfifo fifo/il_P15
mkfifo fifo/il_S1_summary_P15
mkfifo fifo/il_S1_summarypltcalc_P15
mkfifo fifo/il_S1_pltcalc_P15

mkfifo fifo/il_P16
mkfifo fifo/il_S1_summary_P16
mkfifo fifo/il_S1_summarypltcalc_P16
mkfifo fifo/il_S1_pltcalc_P16

mkfifo fifo/il_P17
mkfifo fifo/il_S1_summary_P17
mkfifo fifo/il_S1_summarypltcalc_P17
mkfifo fifo/il_S1_pltcalc_P17

mkfifo fifo/il_P18
mkfifo fifo/il_S1_summary_P18
mkfifo fifo/il_S1_summarypltcalc_P18
mkfifo fifo/il_S1_pltcalc_P18

mkfifo fifo/il_P19
mkfifo fifo/il_S1_summary_P19
mkfifo fifo/il_S1_summarypltcalc_P19
mkfifo fifo/il_S1_pltcalc_P19

mkfifo fifo/il_P20
mkfifo fifo/il_S1_summary_P20
mkfifo fifo/il_S1_summarypltcalc_P20
mkfifo fifo/il_S1_pltcalc_P20


# --- Do insured loss computes ---

pltcalc < fifo/il_S1_summarypltcalc_P1 > work/kat/il_S1_pltcalc_P1 & pid1=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P2 > work/kat/il_S1_pltcalc_P2 & pid2=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P3 > work/kat/il_S1_pltcalc_P3 & pid3=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P4 > work/kat/il_S1_pltcalc_P4 & pid4=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P5 > work/kat/il_S1_pltcalc_P5 & pid5=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P6 > work/kat/il_S1_pltcalc_P6 & pid6=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P7 > work/kat/il_S1_pltcalc_P7 & pid7=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P8 > work/kat/il_S1_pltcalc_P8 & pid8=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P9 > work/kat/il_S1_pltcalc_P9 & pid9=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P10 > work/kat/il_S1_pltcalc_P10 & pid10=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P11 > work/kat/il_S1_pltcalc_P11 & pid11=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P12 > work/kat/il_S1_pltcalc_P12 & pid12=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P13 > work/kat/il_S1_pltcalc_P13 & pid13=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P14 > work/kat/il_S1_pltcalc_P14 & pid14=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P15 > work/kat/il_S1_pltcalc_P15 & pid15=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P16 > work/kat/il_S1_pltcalc_P16 & pid16=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P17 > work/kat/il_S1_pltcalc_P17 & pid17=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P18 > work/kat/il_S1_pltcalc_P18 & pid18=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P19 > work/kat/il_S1_pltcalc_P19 & pid19=$!
pltcalc -s < fifo/il_S1_summarypltcalc_P20 > work/kat/il_S1_pltcalc_P20 & pid20=$!

tee < fifo/il_S1_summary_P1 fifo/il_S1_summarypltcalc_P1 > /dev/null & pid21=$!
tee < fifo/il_S1_summary_P2 fifo/il_S1_summarypltcalc_P2 > /dev/null & pid22=$!
tee < fifo/il_S1_summary_P3 fifo/il_S1_summarypltcalc_P3 > /dev/null & pid23=$!
tee < fifo/il_S1_summary_P4 fifo/il_S1_summarypltcalc_P4 > /dev/null & pid24=$!
tee < fifo/il_S1_summary_P5 fifo/il_S1_summarypltcalc_P5 > /dev/null & pid25=$!
tee < fifo/il_S1_summary_P6 fifo/il_S1_summarypltcalc_P6 > /dev/null & pid26=$!
tee < fifo/il_S1_summary_P7 fifo/il_S1_summarypltcalc_P7 > /dev/null & pid27=$!
tee < fifo/il_S1_summary_P8 fifo/il_S1_summarypltcalc_P8 > /dev/null & pid28=$!
tee < fifo/il_S1_summary_P9 fifo/il_S1_summarypltcalc_P9 > /dev/null & pid29=$!
tee < fifo/il_S1_summary_P10 fifo/il_S1_summarypltcalc_P10 > /dev/null & pid30=$!
tee < fifo/il_S1_summary_P11 fifo/il_S1_summarypltcalc_P11 > /dev/null & pid31=$!
tee < fifo/il_S1_summary_P12 fifo/il_S1_summarypltcalc_P12 > /dev/null & pid32=$!
tee < fifo/il_S1_summary_P13 fifo/il_S1_summarypltcalc_P13 > /dev/null & pid33=$!
tee < fifo/il_S1_summary_P14 fifo/il_S1_summarypltcalc_P14 > /dev/null & pid34=$!
tee < fifo/il_S1_summary_P15 fifo/il_S1_summarypltcalc_P15 > /dev/null & pid35=$!
tee < fifo/il_S1_summary_P16 fifo/il_S1_summarypltcalc_P16 > /dev/null & pid36=$!
tee < fifo/il_S1_summary_P17 fifo/il_S1_summarypltcalc_P17 > /dev/null & pid37=$!
tee < fifo/il_S1_summary_P18 fifo/il_S1_summarypltcalc_P18 > /dev/null & pid38=$!
tee < fifo/il_S1_summary_P19 fifo/il_S1_summarypltcalc_P19 > /dev/null & pid39=$!
tee < fifo/il_S1_summary_P20 fifo/il_S1_summarypltcalc_P20 > /dev/null & pid40=$!

( summarycalc -f  -1 fifo/il_S1_summary_P1 < fifo/il_P1 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P2 < fifo/il_P2 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P3 < fifo/il_P3 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P4 < fifo/il_P4 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P5 < fifo/il_P5 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P6 < fifo/il_P6 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P7 < fifo/il_P7 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P8 < fifo/il_P8 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P9 < fifo/il_P9 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P10 < fifo/il_P10 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P11 < fifo/il_P11 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P12 < fifo/il_P12 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P13 < fifo/il_P13 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P14 < fifo/il_P14 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P15 < fifo/il_P15 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P16 < fifo/il_P16 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P17 < fifo/il_P17 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P18 < fifo/il_P18 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P19 < fifo/il_P19 ) 2>> log/stderror.err  &
( summarycalc -f  -1 fifo/il_S1_summary_P20 < fifo/il_P20 ) 2>> log/stderror.err  &

( eve 1 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P1  ) 2>> log/stderror.err &
( eve 2 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P2  ) 2>> log/stderror.err &
( eve 3 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P3  ) 2>> log/stderror.err &
( eve 4 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P4  ) 2>> log/stderror.err &
( eve 5 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P5  ) 2>> log/stderror.err &
( eve 6 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P6  ) 2>> log/stderror.err &
( eve 7 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P7  ) 2>> log/stderror.err &
( eve 8 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P8  ) 2>> log/stderror.err &
( eve 9 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P9  ) 2>> log/stderror.err &
( eve 10 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P10  ) 2>> log/stderror.err &
( eve 11 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P11  ) 2>> log/stderror.err &
( eve 12 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P12  ) 2>> log/stderror.err &
( eve 13 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P13  ) 2>> log/stderror.err &
( eve 14 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P14  ) 2>> log/stderror.err &
( eve 15 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P15  ) 2>> log/stderror.err &
( eve 16 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P16  ) 2>> log/stderror.err &
( eve 17 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P17  ) 2>> log/stderror.err &
( eve 18 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P18  ) 2>> log/stderror.err &
( eve 19 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P19  ) 2>> log/stderror.err &
( eve 20 20 | getmodel | gulcalc -S100 -L100 -r -a1 -i - | fmcalc -a2 > fifo/il_P20  ) 2>> log/stderror.err &

wait $pid1 $pid2 $pid3 $pid4 $pid5 $pid6 $pid7 $pid8 $pid9 $pid10 $pid11 $pid12 $pid13 $pid14 $pid15 $pid16 $pid17 $pid18 $pid19 $pid20 $pid21 $pid22 $pid23 $pid24 $pid25 $pid26 $pid27 $pid28 $pid29 $pid30 $pid31 $pid32 $pid33 $pid34 $pid35 $pid36 $pid37 $pid38 $pid39 $pid40


# --- Do insured loss kats ---

kat work/kat/il_S1_pltcalc_P1 work/kat/il_S1_pltcalc_P2 work/kat/il_S1_pltcalc_P3 work/kat/il_S1_pltcalc_P4 work/kat/il_S1_pltcalc_P5 work/kat/il_S1_pltcalc_P6 work/kat/il_S1_pltcalc_P7 work/kat/il_S1_pltcalc_P8 work/kat/il_S1_pltcalc_P9 work/kat/il_S1_pltcalc_P10 work/kat/il_S1_pltcalc_P11 work/kat/il_S1_pltcalc_P12 work/kat/il_S1_pltcalc_P13 work/kat/il_S1_pltcalc_P14 work/kat/il_S1_pltcalc_P15 work/kat/il_S1_pltcalc_P16 work/kat/il_S1_pltcalc_P17 work/kat/il_S1_pltcalc_P18 work/kat/il_S1_pltcalc_P19 work/kat/il_S1_pltcalc_P20 > output/il_S1_pltcalc.csv & kpid1=$!
wait $kpid1


rm -R -f work/*
rm -R -f fifo/*

# Stop ktools watcher
kill -9 $pid0
