-module(es3_mnesia).

%%% @doc
%%% es3 backend implemented in distributed mnesia
%%% Uses two tables
%%% `es3_mnesia_chunks` which has content local for each node and contains chunks of data
%%% `es3_mnesia_meta` distributed table contains metadata info for each Key-Data pair
%%%  This is list of node-hash list used to reasemble binary data when read and delete

-behaviour(es3_backend).

-export([start/1, stop/0]).
-export([
        write/2,
        read/1,
        delete/1,
        write_meta/2,
        read_meta/1,
        delete_meta/1
        ]).

-export([clear_all/0]).

-record(chunk, {hash_key :: {es3:key(), binary()},
                chunk :: binary()}).
-record(meta, {key :: es3:key(),
               meta :: es3:meta()
              }).

-spec start(map()) -> ok.
start(#{nodes := Nodes}) ->
    reset_mnesia(Nodes),
    mnesia:create_table(es3_mnesia_chunks,
        [
            {record_name, chunk},
            {local_content, true},
            {disc_copies, [node()]}
        ]),
    mnesia:create_table(es3_mnesia_meta,
                [
                    {record_name, meta},
                    {disc_copies, [node()]}
                ]),
    add_table_copies(Nodes),
    ok.

add_table_copies(Nodes) ->
    mnesia:add_table_copy(es3_mnesia_chunks, node(), disc_copies),
    [mnesia:add_table_copy(es3_mnesia_meta, N, disc_copies)
     || N <- Nodes].

-spec stop() -> ok.
stop() ->
    ok.

-spec write(Key :: es3:key(), Chunk :: binary()) -> ok | {error, Reason :: any()}.
write(Key, Chunk) ->
    HashKey = {Key, hash(Chunk)},
    mnesia:dirty_write(es3_mnesia_chunks, #chunk{hash_key = HashKey, chunk = Chunk}).

-spec read(Key :: es3:key()) -> {ok, [Chunk :: binary()]} | {error, Reason :: any()}.
read(Key) ->
    ChunksRecs = mnesia:dirty_match_object(es3_mnesia_chunks, {chunk, {Key, '_'}, '_'}),
    Chunks = [Chunk || #chunk{chunk = Chunk} <- ChunksRecs],
    {ok, Chunks}.

-spec delete(Key :: es3:key()) -> ok | {error, Reason :: any()}.
delete(Key) ->
    ChunksRecs = mnesia:dirty_match_object(es3_mnesia_chunks, {chunk, {Key, '_'}, '_'}),
    lists:foreach(fun(Chunk) ->
        mnesia:dirty_delete_object(es3_mnesia_chunks, Chunk)
    end, ChunksRecs),
    ok.

-spec write_meta(es3:key(), es3:meta()) -> ok.
write_meta(Key, Meta) when is_list(Meta) ->
    {atomic, ok} = mnesia:transaction(fun() ->
        mnesia:write(es3_mnesia_meta, #meta{key = Key, meta = Meta}, write)
    end),
    ok.

-spec read_meta(es3:key()) -> {ok, es3:meta()} | {error, not_found}.
read_meta(Key) ->
    case mnesia:dirty_read(es3_mnesia_meta, Key) of
        [#meta{meta = Meta}] ->
            {ok, Meta};
        [] ->
            {error, not_found}
    end.

-spec delete_meta(es3:key()) -> ok.
delete_meta(Key) ->
    {atomic, ok} = mnesia:transaction(fun() ->
        mnesia:delete(es3_mnesia_meta, Key, write)
    end),
    ok.

%% Internals

hash(Chunk) ->
    erlang:md5(Chunk).

reset_mnesia(Nodes) ->
    ok = mnesia:start(),
    Node = hd(Nodes),
    {ok, _} = mnesia:change_config(extra_db_nodes, [Node]),
    ok = change_schema_type(node()).

change_schema_type(Node) ->
    case mnesia:change_table_copy_type(schema, Node, disc_copies) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, _, _, _}} ->
            ok;
        {aborted, R} ->
            {error, R}
    end.

% testing
clear_all() ->
    mnesia:clear_table(es3_mnesia_chunks),
    mnesia:clear_table(es3_mnesia_meta).