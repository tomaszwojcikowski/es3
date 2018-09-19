-module(es3).

%%% @doc Erlang Simple Storage API

-type key() :: iodata().
-type chunk_hash() :: binary().
-type meta() :: [{node(), chunk_hash()}].

-export([
        write/2,
        read/1,
        delete/1,
        chunk_binary/2,
        exists/1
        ]).


-export_type([key/0, meta/0, chunk_hash/0]).

%% API

-spec exists(key()) -> boolean().
exists(Key) when is_binary(Key) ->
    case read_meta(Key) of
        {ok, _Meta} -> true;
        {error, not_found} -> false
    end.

-spec write(Key, Object) -> Res when
Key:: key(),
Object :: binary(),
Res :: ok | {error, Reason :: any()}.
write(Key, Object) when is_binary(Key) ->
    case exists(Key) of
        false ->
            Chunks = chunk_binary(get_chunk_size(), Object),
            {ok, MaybeNodesChunks} = write_chunks(Key, Chunks),
            NodesChunks = get_write_results(MaybeNodesChunks),
            true = length(NodesChunks) == length(Chunks),
            ok = write_meta(Key, NodesChunks);
        true ->
            {error, already_exists}
    end.


-spec read(Key) -> {ok, Object} | {error, Reason} when
Key :: key(),
Object :: binary(),
Reason :: any().
read(Key) when is_binary(Key) ->
    case read_meta(Key) of
        {ok, Meta} ->
            {Nodes, Hashes} = lists:unzip(Meta),
            ChunksMap = read_chunks(Nodes, Key),
            Binary = reasemble_binary(Hashes, ChunksMap),
            {ok, Binary};
        Error ->
            Error
    end.

-spec delete(Key) -> Res when
Key :: key(),
Res :: ok | {error, Reason :: any()}.
delete(Key) when is_binary(Key) ->
    case read_meta(Key) of
        {ok, Meta} ->
            {Nodes, _Hashes} = lists:unzip(Meta),
            delete_chunks(Nodes, Key),
            ok = delete_meta(Key);
        {error, not_found} ->
            {error, not_found}
    end.

%% Internals

% write
get_write_results(MaybeNodesChunks) ->
    lists:map(fun({Node, Chunk, ok}) ->
        {Node, Chunk};
                ({Node, Chunk, Error}) ->
        erlang:error({write_failed, Node, Chunk, Error})
    end, MaybeNodesChunks).

write_chunks(Key, Chunks) ->
    es3_nodes:pmap({es3_backend, write, [Key]}, Chunks).

% read
read_chunks(Nodes, Key) ->
    KeyList = [[Key] || _ <- Nodes],
    {ok, MaybeChunksList} = es3_nodes:pmap(Nodes, {es3_backend, read, []}, KeyList),
    ChunksList = get_read_results(MaybeChunksList),
    Chunks = lists:flatten(ChunksList),
    lists:foldl(fun(Chunk, Map) ->
        maps:put(hash(Chunk), Chunk, Map)
    end, #{}, Chunks).

get_read_results(MaybeChunksList) ->
    lists:map(fun({_Node, _Keys, {ok, Chunks}}) ->
        Chunks;
                (Error) ->
        erlang:error({read_result_error, Error})
    end, MaybeChunksList).

reasemble_binary(Hashes, ChunksMap) ->
    lists:foldl(fun(Hash, BinAcc) ->
        case maps:find(Hash, ChunksMap) of
            {ok, Chunk} ->
                <<BinAcc/binary, Chunk/binary>>;
            error ->
                erlang:error({reasemble_binary_failed, Hashes, ChunksMap})
        end
    end, <<>>, Hashes).

% delete
delete_chunks(Nodes, Key) ->
    KeyList = [[Key] || _ <- Nodes],
    {ok, Res} = es3_nodes:pmap(Nodes, {es3_backend, delete, []}, KeyList),
    case lists:all(fun({_, _, ok}) -> true; (_) -> false end, Res) of
        true ->
            ok;
        false ->
            erlang:error({delete_chunks_failed, Res, Key})
    end.

% meta
write_meta(Key, NodesChunks) ->
    Meta = lists:map(fun({Node, Chunk}) ->
        {Node, hash(Chunk)}
    end, NodesChunks),
    es3_backend:write_meta(Key, Meta).

read_meta(Key) ->
    es3_backend:read_meta(Key).

delete_meta(Key) ->
    es3_backend:delete_meta(Key).

hash(Chunk) ->
    erlang:md5(Chunk).

get_chunk_size() ->
    application:get_env(es3, chunk_size, 128).

%% helpers

chunk_binary(Size, Data) ->
    lists:reverse(chunk_binary(Data, Size, [])).

chunk_binary(<<>>, _Size, Acc) ->
    Acc;
chunk_binary(Data, Size, Acc) when byte_size(Data) < Size ->
    [Data | Acc];
chunk_binary(Data, Size, Acc) ->
    case erlang:split_binary(Data, Size) of
        {Chunk, <<>>} ->
            [Chunk | Acc];
        {Chunk, Rest} ->
            chunk_binary(Rest, Size, [Chunk | Acc])
    end.