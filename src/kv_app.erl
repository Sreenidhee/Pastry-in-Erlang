%%% Simple object access service on top of Pastry: associates a key
%%% (a 128-bit id, same space as node ids) with a string.
%%%
%%% Each peer issues one request per second: odd-numbered requests store a
%%% new (key, string) pair, even-numbered ones read back a key this peer
%%% stored before. A lookup waits until at least one store has been
%%% confirmed, so every peer does exactly ceil(n/2) stores and floor(n/2)
%%% lookups. (Only if a peer's first store times out does its lookup turn
%%% become another store.) The node numerically closest to the key stores the value
%%% and copies it to the next 4 closest nodes (its replica set), so the
%%% value survives when the owner fails. When a node's leaf set changes it
%%% copies its values to any new member of their replica sets.
%%%
%%% Replies go straight back to the requester. In failure mode they are
%%% routed back through Pastry to the requester's id instead, so they use
%%% the same lossy, acked links as everything else.
-module(kv_app).

-export([init/1, deliver/5, forward/3, new_leafs/2, handle_info/2]).

-define(TIMEOUT, 5000).   % ms before an unanswered request counts as failed
-define(COPIES, 5).       % owner + 4 replicas

init(Id) ->
    #{id => Id, store => #{}, master => undefined,
      nreq => 0, sent => 0, out => #{}, keys => [], done => false,
      hops => 0, delivered => 0, timeouts => 0,
      puts => 0, gets => 0, get_ok => 0, get_wrong => 0, get_missing => 0}.

%%% ---- Pastry callbacks ----

%% deliver(msg, key): this node is numerically closest to Key.
deliver({put, ReqId, From, Val}, Key, Hops, App = #{store := S}, Ctx = #{self := Self}) ->
    [pastry_node:send(P, {kv_replica, Key, Val})
     || R = {_, P, _} <- replica_set(Key, Ctx), R =/= Self],
    reply(From, {kv_put_ok, ReqId, Hops}, Ctx),
    App#{store := S#{Key => Val}};
deliver({get, ReqId, From}, Key, Hops, App = #{store := S}, Ctx) ->
    reply(From, {kv_get_reply, ReqId, Hops, maps:find(Key, S)}, Ctx),
    App;
deliver({kv_reply, Pid, Inner}, _Key, _Hops, App, _Ctx) ->
    %% A reply routed back to the requester's id. If the requester has died
    %% it lands on a neighbour, which ignores it.
    case Pid =:= self() of
        true -> handle_info(Inner, App);
        false -> App
    end;
deliver({probe, ReqId, From}, _Key, Hops, App, #{self := {Id, _, _}}) ->
    %% used by test_routing to check where a key ends up
    From ! {probe_reply, ReqId, Hops, Id},
    App.

%% forward(msg, key, nextId): called before each hop. The application could
%% change or drop the message; this one passes it on unchanged.
forward(Msg, _Key, _NextId) ->
    {ok, Msg}.

%% newLeafs(leafSet): the leaf set changed (after a failure). Copy each
%% stored value to nodes that just joined its replica set.
new_leafs(Ctx = #{lo := Lo, hi := Hi, old := Old}, App = #{store := S}) ->
    case [R || R = {_, P, _} <- Lo ++ Hi, not lists:keymember(P, 2, Old)] of
        [] -> ok;
        New ->
            maps:foreach(
                fun(K, V) ->
                    [pastry_node:send(P, {kv_replica, K, V})
                     || R = {_, P, _} <- replica_set(K, Ctx), lists:member(R, New)]
                end, S)
    end,
    App.

reply({_, Pid}, Msg, #{ft := false}) ->
    Pid ! Msg;
reply({Id, Pid}, Msg, #{ft := true}) ->
    pastry_node:route({kv_reply, Pid, Msg}, Id).

%% The ?COPIES nodes closest to Key among this node and its leaf set.
replica_set(Key, #{self := Self, lo := Lo, hi := Hi}) ->
    All = lists:ukeysort(2, [Self | Lo ++ Hi]),
    Sorted = lists:sort([{pastry_node:ring_dist(Id, Key), R} || R = {Id, _, _} <- All]),
    [R || {_, R} <- lists:sublist(Sorted, ?COPIES)].

%%% ---- Request workload and replies ----

handle_info({start_requests, N, Master}, App) ->
    erlang:send_after(rand:uniform(1000), self(), kv_tick),
    maybe_done(App#{nreq := N, master := Master});
handle_info(kv_tick, App = #{sent := Sent, nreq := N, keys := [], out := Out})
  when Sent < N, Sent rem 2 =:= 1, map_size(Out) > 0 ->
    %% Lookup turn, but the first store is not confirmed yet: try again soon.
    erlang:send_after(100, self(), kv_tick),
    App;
handle_info(kv_tick, App = #{sent := Sent, nreq := N}) when Sent < N ->
    case Sent + 1 < N of
        true -> erlang:send_after(1000, self(), kv_tick);
        false -> ok
    end,
    issue_request(App);
handle_info({kv_put_ok, ReqId, Hops}, App) ->
    complete(ReqId, Hops, ok, App);
handle_info({kv_get_reply, ReqId, Hops, Res}, App) ->
    complete(ReqId, Hops, Res, App);
handle_info({kv_timeout, ReqId}, App = #{out := Out, timeouts := T}) ->
    case maps:take(ReqId, Out) of
        error -> App;
        {_, Out1} -> maybe_done(App#{out := Out1, timeouts := T + 1})
    end;
handle_info({probe, Key, From}, App) ->
    pastry_node:route({probe, Key, From}, Key),
    App;
handle_info({kv_replica, K, V}, App = #{store := S}) ->
    App#{store := S#{K => V}};
handle_info(_, App) ->
    App.

issue_request(App = #{id := Id, sent := Sent, keys := Keys, out := Out}) ->
    ReqId = make_ref(),
    {Msg, Key, Info} =
        case Sent rem 2 =:= 1 andalso Keys =/= [] of
            true ->
                {K, V} = lists:nth(rand:uniform(length(Keys)), Keys),
                {{get, ReqId, {Id, self()}}, K, {get, V}};
            false ->
                <<K:128>> = erlang:md5(term_to_binary({Id, Sent, ReqId})),
                V = lists:flatten(io_lib:format("value ~b from node ~.16b", [Sent, Id])),
                {{put, ReqId, {Id, self()}, V}, K, {put, K, V}}
        end,
    pastry_node:route(Msg, Key),
    erlang:send_after(?TIMEOUT, self(), {kv_timeout, ReqId}),
    App#{sent := Sent + 1, out := Out#{ReqId => Info}}.

complete(ReqId, Hops, Res, App = #{out := Out, hops := H, delivered := D}) ->
    case maps:take(ReqId, Out) of
        error ->
            App;                                    % reply after timeout
        {{put, K, V}, Out1} ->
            #{puts := P, keys := Keys} = App,
            maybe_done(App#{out := Out1, hops := H + Hops, delivered := D + 1,
                            puts := P + 1, keys := [{K, V} | Keys]});
        {{get, V}, Out1} ->
            Field = case Res of
                {ok, V} -> get_ok;
                {ok, _} -> get_wrong;
                error -> get_missing
            end,
            #{gets := G} = App,
            maybe_done(App#{out := Out1, hops := H + Hops, delivered := D + 1,
                            gets := G + 1, Field := maps:get(Field, App) + 1})
    end.

maybe_done(App = #{done := false, sent := N, nreq := N, out := Out, master := M})
  when map_size(Out) =:= 0, M =/= undefined ->
    Stats = maps:with([hops, delivered, timeouts, puts, gets,
                       get_ok, get_wrong, get_missing, sent], App),
    M ! {peer_done, self(), Stats},
    App#{done := true};
maybe_done(App) ->
    App.
