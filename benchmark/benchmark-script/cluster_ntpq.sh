#!/bin/bash
#for val in {100..129}
#for val in {1..23} {25..108}
for val in {2..3} 12 19 23 {25..28} 30 32 34 37 {61..63} {84..87} {90..93} 95 {97..98} {105..106} 108
do
	ssh node$val "ntpq -p"
done	
