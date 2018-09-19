-module(es3_rest).

%% Standard callbacks.
-export([init/2]).
-export([allowed_methods/2]).
-export([content_types_provided/2]).
-export([content_types_accepted/2]).
-export([resource_exists/2]).
-export([handle_get/2]).
-export([handle_post/2]).
-export([delete_resource/2]).

%% Custom callbacks.

init(Req, Opts) ->
	{cowboy_rest, Req, Opts}.

allowed_methods(Req, State) ->
	{[<<"GET">>, <<"POST">>, <<"DELETE">>], Req, State}.

content_types_provided(Req, State) ->
	{[
		{{<<"application">>, <<"json">>, []}, handle_get}
	], Req, State}.

content_types_accepted(Req, State) ->
	{[{{<<"application">>, <<"json">>, []}, handle_post}],
		Req, State}.

resource_exists(Req, State) ->
	case cowboy_req:binding(key, Req) of
		undefined ->
			{stop, Req, State};
		Key ->
			{es3:exists(Key), Req, State}
	end.

handle_get(Req, State) ->
    Key = cowboy_req:binding(key, Req),
    Body = case es3:read(Key) of
        {ok, Data} ->
           jsx:encode(#{data => Data});
        {error, not_found} ->
            jsx:encode(#{error => not_found, key => Key})
    end,
    {Body, Req, State}.

handle_post(Req, State) ->
    Key = cowboy_req:binding(key, Req),
    handle_post(Key, Req, State).

handle_post(undefined, Req, State) ->
    {false, Req, State};
handle_post(Key, Req, State) ->
    case es3:exists(Key) of
        true ->
            Resp = jsx:encode(#{error => already_exists, key => Key}),
            Req2 = cowboy_req:set_resp_body(Resp, Req),
            {false, Req2, State};
        false ->
            {ok, Body, Req2} = read_body(Req),
            #{data := Data} = jsx:decode(Body, [return_maps, {labels, existing_atom}]),
            ok = es3:write(Key, Data),
            {{true, <<"/",Key/binary>>}, Req2, State}
    end.


delete_resource(Req, State) ->
    case cowboy_req:binding(key, Req) of
        undefined ->
            {false, Req, State};
        Key ->
            case es3:delete(Key) of
                ok ->
                    Resp = jsx:encode(#{result => ok}),
                    Req2 = cowboy_req:set_resp_body(Resp, Req),
                    {true, Req2, State};
                _Other ->
                    {false, Req, State}
            end
    end.

% Private

read_body(Req) ->
    read_body(Req, <<>>).
read_body(Req0, Acc) ->
    case cowboy_req:read_body(Req0) of
        {ok, Data, Req} -> {ok, << Acc/binary, Data/binary >>, Req};
        {more, Data, Req} -> read_body(Req, << Acc/binary, Data/binary >>)
    end.

