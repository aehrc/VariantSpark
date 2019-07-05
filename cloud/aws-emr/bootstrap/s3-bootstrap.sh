#!/bin/bash
set -e -x

# Used to sit in an s3 bucket and will call an arbitrary script, passing in version information
# This is because AWS EMR requires bootstrap actions to be in a bucket, rather than in a VCS.
# e.g. /path/to/this_script.sh --path-prefix "https://github.com/<user>/<repo>/raw/<tag>" --bootstrap-file "/path/to/version/controlled/script.sh" -- <script_arguments>
# Will pass --path-prefix to the called script, so it can reference the relevant version in some VCS.

BOOTSTRAP_LOCAL_FILE="/tmp/bootstrap.sh"

error_out ()
{
    >&2 echo "Error: $1"
    exit 1
}

options=$(getopt -o '' --longoptions path-prefix:,bootstrap-file: -- "$@")
eval set -- "$options"

while true; do
    case "$1" in
    --path-prefix)
        shift
        PATH_PREFIX=$1
        ;;
    --bootstrap-file)
        shift
        BOOTSTRAP_FILE=$1
        ;;
    --)
        shift
        break
        ;;        
    *)
        error_out "unrecognised option: $1"
        ;;
    esac
    shift
done

if [ -z "$PATH_PREFIX" ]; then
    error_out "missing required option: --path-prefix"
fi
if [ -z "$BOOTSTRAP_FILE" ]; then
    error_out "missing required option: --bootstrap-file"
fi

curl -L --output "$BOOTSTRAP_LOCAL_FILE" "${PATH_PREFIX}${BOOTSTRAP_FILE}"
sudo chmod +x "$BOOTSTRAP_LOCAL_FILE"
"$BOOTSTRAP_LOCAL_FILE" --path-prefix "$PATH_PREFIX" $@
