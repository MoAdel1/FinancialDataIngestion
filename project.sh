#!/bin/bash
# $ANACONDA=path where anaconda is installed locally

# activate proper env
source $ANACONDA/etc/profile.d/conda.sh

# define script variables
BRANCH="$(git branch | grep \* | cut -d ' ' -f2)"
USER="$(git config user.name)"
WORKSPACE_DIR="/datapipe/dev/$BRANCH [$USER]"

# main
if  [ "$1" = "help" ]; then
    echo "help: display help"
    echo "test: run test cases"
    echo "install_dep: install project dependencies"
    echo "sync_up: upload jobs to databricks work space"
    echo "sync_down: download jobs from databricks work space"
    echo "sync_delete: delete remote branch from databricks work space"
elif [ "$1" = "test" ]; then
    echo "Starting tests"
    pytest ./tests
elif [ "$1" = "sync_up" ]; then
    echo "uploading"
    databricks workspace import_dir -o -e ./jobs "$WORKSPACE_DIR"
elif [ "$1" = "sync_down" ]; then
    echo "downloading"
    databricks workspace export_dir -o "$WORKSPACE_DIR" ./jobs
elif [ "$1" = "sync_delete" ]; then
    echo "deleting remote branch"
    databricks workspace delete -r "$WORKSPACE_DIR"
elif [ "$1" = "install_dep" ]; then
    echo "Installing project dependencies locally"
    pip install -r tests/requirements.txt
    pip install -r ./requirements.txt --pre
    echo "Adding environment variables"
    conda env config vars set ENV="local"
else
    echo "Must provide an option"
fi
      