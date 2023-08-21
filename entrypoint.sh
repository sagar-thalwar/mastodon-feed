#!/bin/bash
set -e

# Load environment variables from .env file
source .env

# Run the Python script
python mastodon_streaming.py
