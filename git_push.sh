#!/bin/bash
# sed -i 's/\r$//' push_to_git.sh

# automate git  push
# set post message


if [ "$#" == "1" ]
then
    echo "pushing to Git"
    # run all relevant Git commands to sync
    git add -A
    git commit -m "$1"
    git push

else
    echo "You need to specify a commit messsage - check quotes or spaces"
    echo "Examples:"
    echo "  bash $0 1st"
    echo "  bash $0 \"First Commit\""


fi
