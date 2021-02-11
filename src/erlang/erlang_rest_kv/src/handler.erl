%% Feel free to use, reuse and abuse the code in this file.

%% @doc Hello world handler.
-module(handler).

-export([init/2]).
-export([allowed_methods/2, content_types_provided/2, handle_req/2]).

init(Req, State) ->
	{cowboy_rest, Req, State}.

allowed_methods(Req, State) ->  
	{[<<"GET">>, <<"POST">>, <<"PUT">>, <<"DELETE">>], Req, State}.

content_types_provided(Req, State) ->
	{[
		{<<"application/json">>, handle_req}
	], Req, State}.

handle_req(Req, State) ->
	Method = cowboy_req:method(Req),
	Body = reply(Method, Req),
	{Body, Req, State}.


reply(<<"GET">>, Req) ->
	logger:notice("Received GET Request"),
	#{key := Key} = cowboy_req:match_qs([{key, [], undefined}], Req),
	reply_get(Key);

reply(Method, Req) when ( Method == <<"POST">> ) or ( Method == <<"PUT">> ) ->
	logger:notice("Received POST/PUT Request"),
	{ok, PostVals, _} = cowboy_req:read_urlencoded_body(Req),
	Key = proplists:get_value(<<"key">>, PostVals),
	Value = proplists:get_value(<<"value">>, PostVals),
	reply_store(Key, Value);

reply(<<"DELETE">>, Req) ->
	logger:notice("Received DELETE Request"),
	{ok, PostVals, _} = cowboy_req:read_urlencoded_body(Req),
	Key = proplists:get_value(<<"key">>, PostVals),
	reply_delete(Key).

reply_get(undefined) ->
	Dict = kv_store:get_all(),
	jsone:encode(Dict);

reply_get(Key) ->
	Value = kv_store:get(Key),
	jsone:encode(Value).

reply_store(Key, Value) ->
	kv_store:set(Key, Value),	
	jsone:encode([{<<"success">>, <<"true">>}]).

reply_delete(Key) ->
	kv_store:delete(Key),
	jsone:encode([{<<"success">>, <<"true">>}]).