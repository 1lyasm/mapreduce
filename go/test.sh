#!/bin/bash

SRC=$(pwd)/src

ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}

TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

failed_any=0

echo '***' Starting wc test.

$SRC/main/mrsequential $SRC/mrapps/wc.so $SRC/test/pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

maybe_quiet $TIMEOUT $SRC/main/mrcoordinator $SRC/test/pg*txt &
pid=$!

sleep 1

(maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/wc.so) &
(maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/wc.so) &
(maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/wc.so) &

wait $pid

sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

wait

echo '***' Starting indexer test.

rm -f mr-*

$SRC/main/mrsequential $SRC/mrapps/indexer.so $SRC/test/pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

maybe_quiet $TIMEOUT $SRC/main/mrcoordinator $SRC/test/pg*txt &
sleep 1

maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/indexer.so &
maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

echo '***' Starting map parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT $SRC/main/mrcoordinator $SRC/test/pg*txt &
sleep 1

maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/mtiming.so &
maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait

echo '***' Starting reduce parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT $SRC/main/mrcoordinator $SRC/test/pg*txt &
sleep 1

maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/rtiming.so  &
maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

echo '***' Starting job count test.

rm -f mr-*

maybe_quiet $TIMEOUT $SRC/main/mrcoordinator $SRC/test/pg*txt  &
sleep 1

maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/jobcount.so &
maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/jobcount.so
maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/jobcount.so &
maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/jobcount.so

NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait

echo '***' Starting early exit test.

rm -f mr-*

DF=anydone$$
rm -f $DF

(maybe_quiet $TIMEOUT $SRC/main/mrcoordinator $SRC/test/pg*txt; touch $DF) &

sleep 1

(maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT $SRC/main/mrworker $SRC/mrapps/early_exit.so; touch $DF) &

jobs &> /dev/null
wait -n

rm -f $DF

sort mr-out* | grep . > mr-wc-all-initial

wait

sort mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f mr-*

echo '***' Starting crash test.

$SRC/main/mrsequential $SRC/mrapps/nocrash.so $SRC/test/pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
((maybe_quiet $TIMEOUT2 $SRC/main/mrcoordinator $SRC/test/pg*txt); touch mr-done ) &
sleep 1

maybe_quiet $TIMEOUT2 $SRC/main/mrworker $SRC/mrapps/crash.so &

SOCKNAME=/var/tmp/1234-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 $SRC/main/mrworker $SRC/mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 $SRC/main/mrworker $SRC/mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  maybe_quiet $TIMEOUT2 $SRC/main/mrworker $SRC/mrapps/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
