#!/bin/sh
# Pass an isolated marker path as the first argument; invoke through sh.
printf '%s\n' plugin-executed > "$1"
