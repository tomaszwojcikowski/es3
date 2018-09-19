-module(es3_backend).

-export([start/1, stop/0]).
-export([write/2, read/1, delete/1, write_meta/2, read_meta/1, delete_meta/1]).
-export([clear_all/0]).

-callback start(Args::term()) -> ok.
-callback stop() -> ok.

-callback write(Key :: es3:key(), Chunk :: binary()) -> ok | {error, Reason :: any()}.
-callback read(Key :: es3:key()) -> {ok, [Chunk :: binary()]} | {error, Reason :: any()}.
-callback delete(Key :: es3:key()) -> ok | {error, Reason :: any()}.

-callback write_meta(Key :: es3:key(), Meta :: es3:meta()) -> ok | {error, term()}.
-callback read_meta(Key :: es3:key()) -> {ok, Meta :: es3:meta()} | {error, term()}.
-callback delete_meta(Key :: es3:key()) -> ok.

-define(BACKEND, (backend())).

start(Args) -> ?BACKEND:start(Args).
stop() -> ?BACKEND:stop().
write(Key, Chunk) -> ?BACKEND:write(Key, Chunk).
read(Key) -> ?BACKEND:read(Key).
delete(Key) -> ?BACKEND:delete(Key).
write_meta(Key, Meta) -> ?BACKEND:write_meta(Key, Meta).
read_meta(Key) -> ?BACKEND:read_meta(Key).
delete_meta(Key) -> ?BACKEND:delete_meta(Key).

clear_all() -> ?BACKEND:clear_all().

backend() ->
    application:get_env(es3, backend, es3_mnesia).