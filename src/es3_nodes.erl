-module(es3_nodes).

-export([pmap/2]).
-export([pmap/3]).

-define(TIMEOUT, timer:seconds(5)).
-define(CHUNK_SIZE, 16).

%% @doc
%% Run @{MFA} on with each argument from @Args as last argument on nodes in cluster
%% Each execution in run on next node on the list using rpc
%% Calls are run in parallel using spawned processes in chunks of 16
%% Default timeout of each call is 5s
%% @Returns
%% {ok, [{Node, Arg, Results}]}
%% Where
%% @Node is node name on which node call was run with @Arg returning @Result
-spec pmap({module(), atom(), list()}, list()) -> {ok, [{node(), term(), term()} | {error, timeout}]}.
pmap({M, F, A}, Args) ->
    pmap(get_nodes(), {M, F, A}, Args).

%% @doc
%% Same as pmap/2 but runs only on provided @Nodes
-spec pmap([node()], {module(), atom(), [term()]}, [term()]) -> {ok, [{node(), term(), term()} | {error, timeout}]}.
pmap(Nodes, {M, F, A}, Args) ->
    ArgsChunks = chunks(Args, ?CHUNK_SIZE),
    DeepList = lists:map(fun(ArgsChunk) ->
        {ok, Res} = do_pmap(Nodes, {M, F, A}, ArgsChunk),
        Res
    end, ArgsChunks),
    {ok, lists:flatten(DeepList)}.

do_pmap([], _, _) ->
    erlang:error({do_pmap_failed, no_nodes});
do_pmap(Nodes, {M, F, A}, Args) ->
    {NodesArgs, _} = lists:foldl(fun(Arg, {Acc, Idx}) ->
        Node = choose_node(Idx, Nodes),
        {Pid, Ref} = spawn(Node, M, F, A, Arg),
        {[{Pid, Ref, Node, Arg} | Acc], Idx+1}
    end, {[], 1}, Args),
    gather(lists:reverse(NodesArgs), []).

choose_node(Idx, Nodes) ->
    L = length(Nodes),
    lists:nth((Idx rem L) + 1, Nodes).

spawn(Node, M, F, A, Arg) ->
    erlang:spawn_monitor(
            fun() ->
                Res = apply_fun(Node, M, F, A, Arg),
                exit(Res)
            end).

gather([], Acc) ->
    {ok, lists:reverse(Acc)};
gather([{Pid, Ref, Node, Arg} | NodesArgs], Acc) ->
    receive
        {'DOWN', Ref, process, Pid, Result} ->
            gather(NodesArgs, [{Node, Arg, Result} | Acc])
    after ?TIMEOUT ->
        {error, timeout}
    end.

apply_fun(Node, M, F, List, Args) when is_list(Args)->
    case rpc:call(Node, M, F, List ++ Args) of
        {badrpc, Error} ->
            {error, badrpc, Error};
        Result ->
            Result
    end;
apply_fun(Node, M, F, List, Arg) ->
    apply_fun(Node, M, F, List, [Arg]).

get_nodes() ->
    lists:sort([node() | nodes()]).

chunks([], _N) ->
    [];
chunks(List, N) ->
    chunks(List, N, 0, []).

chunks([], _, _, [List | Acc]) ->
    lists:reverse([lists:reverse(List) | Acc]);
chunks([Item|Rest], N, 0, Acc) ->
    chunks(Rest, N, 1, [[Item]|Acc]);
chunks([Item|Rest], N, N, [List|Acc]) ->
    chunks(Rest, N, 1, [[Item],lists:reverse(List)|Acc]);
chunks([Item|Rest], N, M, [List|Acc]) when M < N ->
    chunks(Rest, N, M+1, [[Item|List]|Acc]).
