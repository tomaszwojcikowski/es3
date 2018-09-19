-module(es3_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

suite() ->
    [{timetrap,{seconds,30}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(es3),
    Config.

end_per_suite(_Config) ->
    application:stop(es3),
    ok.

init_per_group(dist, Config) ->
    [start_slave(N) || N <- slaves()],
    Nodes = [node() | nodes()],
    [init_slave(N, Nodes) || N <- nodes()],
    Config;
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(dist, Config) ->
    [ct_slave:stop(N) || N <- slaves()],
    Config;
end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    es3_backend:clear_all(),
    ok.

groups() ->
    [
        {single, tests()},
        {dist,
            [cluster_test | tests()]
        }
    ].

tests() ->
    [
        write_test,
        write_multi_keys,
        write_duplicate_test,
        not_found_test,
        delete_test,
        big_data_test,
        rest_post_test,
        rest_not_found_test,
        rest_delete_test,
        rest_delete_not_found
    ].

all() ->
    [
        {group, single},
        {group, dist}
    ].

%% tests
write_test(_Config) ->
    Key = random_key(),
    Data = random_data(),
    ok = es3:write(Key, Data),
    {ok, Data} = es3:read(Key).

write_multi_keys(_) ->
    Data = random_data(),
    lists:foreach(fun(_) ->
        Key = random_key(),
        ok = es3:write(Key, Data),
        {ok, Data} = es3:read(Key)
    end, lists:seq(1, 50)).

write_duplicate_test(_Config) ->
    Key = random_key(),
    Data = random_data(),
    ok = es3:write(Key, Data),
    {error, already_exists} = es3:write(Key, Data).

not_found_test(_Config) ->
    {error, not_found} = es3:read(random_key()).

delete_test(_Config) ->
    Key = random_key(),
    Data = random_data(),
    {error, not_found} = es3:read(Key),
    ok = es3:write(Key, Data),
    {ok, Data} = es3:read(Key),
    ok = es3:delete(Key),
    {error, not_found} = es3:read(Key).

big_data_test(_) ->
    Key = random_key(),
    Data = random_data(1024*10),
    ok = es3:write(Key, Data),
    {ok, Data} = es3:read(Key).

cluster_test(_) ->
    Nodes = lists:sort([node() | nodes()]),
    lists:foreach(fun(N) ->
        MNs = ct_rpc:call(N, mnesia, table_info, [es3_mnesia_meta, disc_copies]),
        Nodes = lists:sort(MNs)
    end, Nodes),
    ok.

%% rest tests

rest_post_test(_Config) ->
    Key = "key",
    Data = random_data(),
    Req = {rest_url(Key), [], "application/json", jsx:encode(#{data => Data})},
    assert_rest_code(201, httpc:request(post, Req, [], [])),
    {ok,{{"HTTP/1.1",200,"OK"},_,JSON}} = httpc:request(get, {rest_url(Key), []}, [], []),
    #{data := Data} = jsx:decode(list_to_binary(JSON), [return_maps, {labels, atom}]).

rest_not_found_test(_) ->
    {ok,{{"HTTP/1.1",404,_},_,_}} = httpc:request(get, {rest_url("undef"), []}, [], []).

rest_delete_test(_) ->
    Key = "key",
    Data = random_data(),
    Req = {rest_url(Key), [], "application/json", jsx:encode(#{data => Data})},
    assert_rest_code(201, httpc:request(post, Req, [], [])),
    assert_rest_code(200, httpc:request(get, {rest_url(Key), []}, [], [])),
    assert_rest_code(200, httpc:request(delete, {rest_url(Key), []}, [], [])),
    assert_rest_code(404, httpc:request(get, {rest_url(Key), []}, [], [])).

rest_delete_not_found(_) ->
    assert_rest_code(404, httpc:request(delete, {rest_url("undef"), []}, [], [])).

%% helpers

assert_rest_code(Code, {ok,{{"HTTP/1.1",Code,_},_,_}}) -> ok.

rest_url(Key) ->
   "http://localhost:8080/" ++ Key.

random_key() ->
    crypto:strong_rand_bytes(10).

random_data() ->
    random_data(1024).
random_data(N) ->
    base64:encode(crypto:strong_rand_bytes(N)).

%%% Dist

slaves() ->
    [a, b, c].

random_port() ->
    9000 + erlang:phash2(os:timestamp(), 100).

start_slave(Node) ->
    case ct_slave:start(Node,[{monitor_master, true}]) of
        {error, Reason, Name}
            when Reason == started_not_connected;
                  Reason == already_started ->
            pong = net_adm:ping(Name);
        {ok, _Name} ->
            ok
    end.

init_slave(Node, NodesToConnect) ->
    ct:log("init slave ~p", [Node]),
    ct_rpc:call(Node, code, add_paths, [code:get_path()]),
    ok = ct_rpc:call(Node, application, set_env, [es3, nodes, NodesToConnect]),
    ok = ct_rpc:call(Node, application, set_env, [es3, rest_port, random_port()]),
    {ok, _} = ct_rpc:call(Node, application, ensure_all_started, [es3]).