%%% Checks the Pastry correctness property: every routed key is delivered to
%%% the live node whose id is numerically closest to it.
%%%   erl -noshell -pa ebin -run test_routing main NumNodes NumProbes [KillPct]
-module(test_routing).
-export([main/1]).

main([NS, PS | Rest]) ->
    N = list_to_integer(NS),
    Probes = list_to_integer(PS),
    Kill = case Rest of [K] -> list_to_integer(K); [] -> 0 end,
    {All, _} = project3:build(N, #{ft => Kill > 0, drop => 0.0}),
    Victims = lists:sublist(shuffle(All), N * Kill div 100),
    [exit(P, kill) || {_, P, _} <- Victims],
    timer:sleep(case Kill of 0 -> 0; _ -> 2000 end),   % let repair finish
    Live = All -- Victims,
    LiveIds = [Id || {Id, _, _} <- Live],
    LiveT = list_to_tuple(Live),
    Res = [probe(element(2, element(rand:uniform(tuple_size(LiveT)), LiveT)), LiveIds)
           || _ <- lists:seq(1, Probes)],
    Ok = length([x || {true, _} <- Res]),
    Hops = lists:sum([H || {_, H} <- Res]),
    io:format("~b nodes (~b killed): ~b/~b probes reached the closest live node, "
              "avg ~.3f hops~n", [N, length(Victims), Ok, Probes, Hops / Probes]),
    erlang:halt(0).

probe(Pid, LiveIds) ->
    <<Key:128>> = erlang:md5(term_to_binary({make_ref(), rand:uniform()})),
    Pid ! {probe, Key, self()},
    receive
        {probe_reply, Key, Hops, Id} ->
            {_, Best} = lists:min([{pastry_node:ring_dist(I, Key), I} || I <- LiveIds]),
            {Best =:= Id, Hops}
    after 5000 -> {false, 0}
    end.

shuffle(L) -> [X || {_, X} <- lists:sort([{rand:uniform(), X} || X <- L])].
