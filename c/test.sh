#!/usr/bin/bash
mkdir -p mr-tmp
cd mr-tmp
time ../bin/mrsequential ../../test/*.txt
if [$(diff mr-out-0 ../../test/mr-correct-wc) = ""]; then
	echo "mrsequential: wc: PASSED"
else
	echo "mrsequential: wc: FAILED"
	exit 1
fi

