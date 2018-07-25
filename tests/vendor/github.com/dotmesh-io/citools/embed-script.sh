#!/bin/sh

# Generate dindscript.go from dind-cluster-patched.sh

(echo 'package citools'
 echo ''
 echo 'const DIND_SCRIPT = `'
 sed 's/`/`+"`"+`/g' < dind-cluster-patched.sh
 echo '`') > dindscript.go
