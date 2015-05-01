#!/bin/bash
dir=$(dirname $0 )


cat "$dir/tomita-test/person.txt" | ./tomita-linux64 "$dir/tomita-test/config.proto"  > "$dir/tomita-test/person.out"
