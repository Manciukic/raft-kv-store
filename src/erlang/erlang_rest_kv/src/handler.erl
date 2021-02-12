%% Feel free to use, reuse and abuse the code in this file.

%% @doc Hello world handler.
-module(handler).

-export([init/2]).
-export([allowed_methods/2, content_types_provided/2, handle_req/2, content_types_accepted/2, delete_resource/2]).

init(Req, State) ->
	{cowboy_rest, Req, State}.

allowed_methods(Req, State) ->  
	{[<<"GET">>, <<"POST">>, <<"PUT">>, <<"DELETE">>], Req, State}.

content_types_provided(Req, State) ->
	{[
		{<<"application/json">>, handle_req}
	], Req, State}.

content_types_accepted(Req, State) ->
	{[{{<<"application">>, <<"x-www-form-urlencoded">>, '*'}, handle_req}],
		Req, State}.

handle_req(Req, State) ->
	logger:debug("handle_req(~p, ~p)~n", [Req, State]),
	Method = cowboy_req:method(Req),
	Body = reply(Method, Req),
	{Body, Req, State}.

delete_resource(Req, State) ->
	handle_req(Req, State).


reply(<<"GET">>, Req) ->
	logger:debug("Received GET Request"),
	#{key := Key} = cowboy_req:match_qs([{key, [], undefined}], Req),
	reply_get(Key);

reply(Method, Req) when ( Method == <<"POST">> ) or ( Method == <<"PUT">> ) ->
	logger:debug("Received POST/PUT Request"),
	{ok, PostVals, _} = cowboy_req:read_urlencoded_body(Req),
	logger:debug("PostVals:~p~n", [PostVals]),
	Key = proplists:get_value(<<"key">>, PostVals),
	Value = proplists:get_value(<<"value">>, PostVals),
	reply_store(Key, Value);

reply(<<"DELETE">>, Req) ->
	logger:debug("Received DELETE Request"),
	{ok, PostVals, _} = cowboy_req:read_urlencoded_body(Req),
	Key = proplists:get_value(<<"key">>, PostVals, null),
	All = proplists:get_value(<<"all">>, PostVals, false),
	case All of 
		false ->
			reply_delete(Key);
		_ ->
			reply_delete_all()
	end.

reply_get(undefined) ->
	logger:debug("get~n", []),
	Dict = kv_store:get_all(),
	jsone:encode(Dict);

reply_get(Key) ->
	logger:debug("get(~p)~n", [Key]),
	Value = kv_store:get(Key),
	jsone:encode(Value).

reply_store(Key, Value) ->
	logger:debug("set(~p, ~p)~n", [Key, Value]),	
	kv_store:set(Key, Value),	
	true.

reply_delete(Key) ->
	logger:debug("delete(~p)~n", [Key]),	
	kv_store:delete(Key),
	true.

reply_delete_all() ->
	logger:debug("delete_all()~n", []),	
	kv_store:delete_all(),
	true.
