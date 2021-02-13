#!/bin/sh

export RELX_REPLACE_OS_VARS=true
export NODE_NAME=${1:-erlang_rest_kv}

_rel/erlang_rest_kv/bin/erlang_rest_kv console