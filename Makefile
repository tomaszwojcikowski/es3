.PHONY: test rel clean dialyzer shell

test: rebar3
	./rebar3 ct --sname ct

rel: rebar3
	./rebar3 release

clean: rebar3
	./rebar3 clean

dialyzer: rebar3
	./rebar3 dialyzer

shell: rebar3
	./rebar shell

rebar3:
	wget https://github.com/erlang/rebar3/releases/download/3.5.0/rebar3 && chmod +x rebar3