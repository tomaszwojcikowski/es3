%%%-------------------------------------------------------------------
%% @doc es3 public API
%% @end
%%%-------------------------------------------------------------------

-module(es3_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).
%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    es3_backend:start(#{nodes => get_nodes()}),
    init_rest(),
    es3_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================


get_nodes() ->
    application:get_env(es3, nodes, [node()]).


init_rest() ->
    Port = application:get_env(es3, rest_port, 8080),
    Dispatch = cowboy_router:compile([
		{'_', [
			{"/[:key]", es3_rest, []}
		]}
	]),
	{ok, _} = cowboy:start_clear(http, [{port, Port}], #{
		env => #{dispatch => Dispatch}
	}).