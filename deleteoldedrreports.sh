#!/bin/bash

# Set the base directory
base_dir="/home/ddp/clientdbts"

# Find and delete files older than 7 days
find "$base_dir" -type f -path "*/dbtrepo/*.html" -name "*.????-??-??.html" -mtime +7 -exec rm {} \;
