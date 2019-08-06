#!/bin/zsh

xterm -T "A" -e python3 Lsr.py configA.txt &
xterm -T "B" -e python3 Lsr.py configB.txt &
xterm -T "C" -e python3 Lsr.py configC.txt &
xterm -T "D" -e python3 Lsr.py configD.txt &
xterm -T "E" -e python3 Lsr.py configE.txt &
xterm -T "F" -e python3 Lsr.py configF.txt &
