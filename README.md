# Simple Erlang distributed storage

## Config

```erlang

    {env,[
        {backend, es3_mnesia_backend}, % backend module, defaults to distributed mnesia
        {nodes, [node()]}, % list of nodes in the cluster, defaults to current node
        {chunk_size, 128}, % default size of chunk in bytes
        {rest_port, 8080} % port of rest interface
    ]}
```

## Dependencies

* Cowboy 2.2.0
* JSX 2.9.0

## Build

    make release

## Run

    make shell

## Test

    make test dialyzer

## Erlang Interface

```erlang
ok = es3:write(<<"key">>, <<"data">>).
{ok, Data} = es3:read(<<"key">>).
ok = es3:delete(<<"key">>).
```

## REST Interface

### GET

`curl http://localhost:8080/<key_name> -H Content-Type=-H 'Content-Type: application/json'`

### POST

`curl http://localhost:8080/<key_name>    -H 'Content-Type: application/json'  -d '{"data": "123456789101112131415"}'`

### DELETE

`curl -X DELETE http://localhost:8080/<key_name> -H Content-Type=-H 'Content-Type: application/json'`