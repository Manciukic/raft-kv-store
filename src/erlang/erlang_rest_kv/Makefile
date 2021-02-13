PROJECT = erlang_rest_kv
PROJECT_DESCRIPTION = Cowboy REST Hello World example
PROJECT_VERSION = 1

DEPS = cowboy jsone raft_kv_store
dep_cowboy_commit = master
dep_raft_kv_store = ln ../../raft_kv_store

export RELX_REPLACE_OS_VARS=true
export NODE_NAME=erlang_rest_kv

include ./erlang.mk
