#!/bin/sh

export RELX_REPLACE_OS_VARS=true
export NODE_NAME=${1:-raft_kv_store}

_rel/raft_kv_store/bin/raft_kv_store console