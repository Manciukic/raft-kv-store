%% Feel free to use, reuse and abuse the code in this file.

%% @doc Hello world handler.
-module(handler).

-export([init/2]).
-export([allowed_methods/2, content_types_provided/2, handle_get/2, handle_store/2, content_types_accepted/2, delete_resource/2]).

init(Req, State) ->
	{cowboy_rest, Req, State}.

allowed_methods(Req, State) ->  
	{[<<"GET">>, <<"POST">>, <<"PUT">>, <<"DELETE">>], Req, State}.


% GET

content_types_provided(Req, State) ->
	{[
		{<<"application/json">>, handle_get}
	], Req, State}.


handle_get(Req, State) ->
	logger:debug("Received GET Request"),
	#{key := Key} = cowboy_req:match_qs([{key, [], undefined}], Req),
	{ reply_get(Key), Req, State }.


reply_get(undefined) ->
	logger:debug("get_all~n", []),
	Result = kv_store:get_all(),
	Data = case Result of
		error -> [ get_success_body(false) ];
		_ -> [ get_success_body(true), {<<"data">>, Result } ]
	end,
	jsone:encode(Data);

reply_get(Key) ->
	logger:debug("get(~p)~n", [Key]),
	Value = kv_store:get(Key),
	Data = case Value of
		error -> [ get_success_body(false) ];
		_ -> [ get_success_body(true), {<<"data">>, [{ Key, Value }]} ]
	end,
	logger:debug("get(~p)~n", [Data]),
	jsone:encode(Data).
	
	
% POST / PUT
content_types_accepted(Req, State) ->
	{[{{<<"application">>, <<"x-www-form-urlencoded">>, '*'}, handle_store}],
		Req, State}.

handle_store(Req, State) ->
	logger:debug("Received POST/PUT Request"),
	{ok, PostVals, _} = cowboy_req:read_urlencoded_body(Req),
	logger:debug("PostVals:~p~n", [PostVals]),
	Key = proplists:get_value(<<"key">>, PostVals),
	Value = proplists:get_value(<<"value">>, PostVals),
	Success = reply_store(Key, Value),
	Resp = cowboy_req:set_resp_body(get_encoded_success_body(Success), Req),
	{get_boolean(Success), Resp, State}.

reply_store(Key, Value) ->
	logger:debug("set(~p, ~p)~n", [Key, Value]),	
	kv_store:set(Key, Value).

% DELETE
delete_resource(Req, State) ->
	logger:debug("Received DELETE Request"),
	{ok, PostVals, _} = cowboy_req:read_urlencoded_body(Req),
	Key = proplists:get_value(<<"key">>, PostVals, false),
	All = proplists:get_value(<<"all">>, PostVals, false),
	Success = case All of 
		false ->
			case Key of
				false -> error;
				_ -> reply_delete(Key)
			end;
		_ ->
			reply_delete_all()
	end,
	Resp = cowboy_req:set_resp_body(get_encoded_success_body(Success), Req),
	{ok, Resp, State}.

reply_delete(Key) ->
	logger:debug("delete(~p)~n", [Key]),
	% Possible results are ok and error	
	kv_store:delete(Key).

reply_delete_all() ->
	logger:debug("delete_all()~n", []),	
	% Possible results are ok and error	
	kv_store:delete_all().

% UTILITY
get_success_body(Success) when ( Success == ok ) or ( Success == true ) ->
	{<<"success">>, <<"true">>};

get_success_body(_) ->
	{<<"success">>, <<"false">>}.

get_encoded_success_body(Success) ->
	jsone:encode([get_success_body(Success)]).

get_boolean(error) -> false;
get_boolean(ok) -> true;
get_boolean(_) -> false.