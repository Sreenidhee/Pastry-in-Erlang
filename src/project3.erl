%%% Driver: builds the Pastry network, runs the request workload, prints
%%% the average number of hops.
%%%
%%%   project3 [-q] numNodes numRequests [failPct [linkFailPct [dropPct]]]
%%%
%%% -q prints only the "Average number of hops" line.
%%%
%%% The optional arguments enable the failure model (bonus):
%%%   failPct      percent of nodes killed halfway through the requests
%%%   linkFailPct  percent of each node's links that fail permanently
%%%                at the same moment
%%%   dropPct      percent chance that any single transmission between
%%%                nodes is lost (temporary link failure), from the start
%%%                of the requests to the end of the run
-module(project3).

-export([start/0, main/1, build/2]).

%% Entry point used by the ./project3 launcher (arguments after -extra).
start() ->
    main(init:get_plain_arguments()).

main(["-q" | Args]) ->
    put(quiet, true),
    main(Args);
main(Args) ->
    case parse(Args) of
        {ok, Cfg} ->
            run(Cfg),
            erlang:halt(0);
        error ->
            io:format("usage: project3 [-q] numNodes numRequests "
                      "[failPct [linkFailPct [dropPct]]]~n"
                      "  numNodes, numRequests >= 1; 0 <= failPct, linkFailPct <= 100; "
                      "0 <= dropPct < 100~n"),
            erlang:halt(1)
    end.

parse([N, R | Opt]) when length(Opt) =< 3 ->
    try
        Nodes = list_to_integer(N),
        Reqs = list_to_integer(R),
        [F, L, D] = [num(X) || X <- Opt] ++ lists:duplicate(3 - length(Opt), 0),
        true = Nodes >= 1 andalso Reqs >= 1,
        true = F >= 0 andalso F =< 100 andalso L >= 0 andalso L =< 100,
        true = D >= 0 andalso D < 100,
        {ok, #{n => Nodes, r => Reqs, fail => F, link => L, drop => D}}
    catch _:_ -> error
    end;
parse(_) -> error.

%% Progress and detail output; suppressed by -q.
log(Fmt, Args) ->
    case get(quiet) of
        true -> ok;
        _ -> io:format(Fmt, Args)
    end.

num(S) ->
    try list_to_integer(S)
    catch _:_ -> list_to_float(S)
    end.

run(Cfg = #{n := N, r := R, fail := F, link := LF, drop := D}) ->
    Failures = F > 0 orelse LF > 0,
    FT = Failures orelse D > 0,
    NodeCfg = #{ft => FT},
    log("Pastry: ~b nodes, ~b requests per node~n", [N, R]),
    case FT of
        true -> log("Failure model: kill ~p% of nodes, break ~p% of links, "
                          "lose ~p% of sends~n", [F, LF, D]);
        false -> ok
    end,

    T0 = erlang:monotonic_time(millisecond),
    {Refs, JoinHops} = build(N, NodeCfg),
    Pids = [P || {_, P, _} <- Refs],
    T1 = erlang:monotonic_time(millisecond),
    log("Network built in ~.2f s (average join path ~.2f hops)~n",
              [(T1 - T0) / 1000, JoinHops / max(1, N - 1)]),

    case D > 0 of
        true -> [P ! {'$set_drop', D / 100} || P <- Pids];   % lossy links from now on
        false -> ok
    end,
    [P ! {start_requests, R, self()} || P <- Pids],
    case Failures of
        true -> erlang:send_after(max(1, R div 2) * 1000 + 500, self(), inject_failures);
        false -> ok
    end,
    Waiting = maps:from_list([{P, true} || P <- Pids]),
    {Stats, Killed} = collect(Waiting, [], [], Pids, Cfg),
    T2 = erlang:monotonic_time(millisecond),
    report(Stats, Killed, Pids, N, Cfg, (T2 - T1) / 1000).

%% Nodes join one after another; each joins through a nearby existing node.
%% Returns the node refs {Id, Pid, Coord} and the total join path length.
build(N, NodeCfg) ->
    build(1, N, NodeCfg, #{}, 0).

build(I, N, _, Map, Hops) when I > N ->
    {maps:values(Map), Hops};
build(I, N, NodeCfg, Map, Hops) ->
    <<Id:128>> = erlang:md5(integer_to_binary(I)),
    Coord = (rand:uniform(1 bsl 20) - 1) bsl 20 + rand:uniform(1 bsl 20) - 1,
    Pid = pastry_node:start(Id, Coord, self(), NodeCfg),
    Boot = case I of 1 -> none; _ -> pick_boot(Coord, Map, I - 1) end,
    Pid ! {'$start_join', Boot},
    H = receive
            {joined, Pid, JH} -> JH
        after 30000 ->
            io:format("node ~b failed to join~n", [I]),
            erlang:halt(2)
        end,
    case N >= 1000 andalso I rem (N div 10) =:= 0 of
        true -> log("  ~b nodes joined~n", [I]);
        false -> ok
    end,
    build(I + 1, N, NodeCfg, Map#{I => {Id, Pid, Coord}}, Hops + H).

%% The paper assumes a new node knows a nearby Pastry node. We approximate
%% that by taking the closest of a few random existing nodes.
pick_boot(Coord, Map, K) ->
    Sample = [maps:get(rand:uniform(K), Map) || _ <- lists:seq(1, 8)],
    {_, {_, Pid, _}} = lists:min([{pastry_node:prox({0, 0, Coord}, R), R} || R <- Sample]),
    Pid.

collect(W, Acc, Killed, _, _) when map_size(W) =:= 0 ->
    {Acc, Killed};
%% Peers report only when all their requests are done (about numRequests
%% seconds), so the idle timeout scales with numRequests.
collect(W, Acc, Killed, All, Cfg = #{fail := F, link := LF, r := R}) ->
    receive
        {peer_done, P, S} ->
            collect(maps:remove(P, W), [S | Acc], Killed, All, Cfg);
        inject_failures ->
            NKill = min(length(All) - 1, round(length(All) * F / 100)),
            Victims = lists:sublist(shuffle(All), NKill),
            [exit(P, kill) || P <- Victims],
            Survivors = All -- Victims,
            case LF > 0 of
                true -> [P ! {'$break_links', LF / 100} || P <- Survivors];
                false -> ok
            end,
            log("Failures injected: ~b nodes killed, ~p% of links broken~n",
                      [NKill, LF]),
            collect(maps:without(Victims, W), Acc, Victims, All, Cfg)
    after (R + 60) * 1000 ->
        io:format("Gave up waiting for ~b peers~n", [map_size(W)]),
        {Acc, Killed}
    end.

report(Stats, Killed, All, N, #{drop := D, fail := F, link := LF}, Secs) ->
    Sum = fun(K) -> lists:sum([maps:get(K, S) || S <- Stats]) end,
    Delivered = Sum(delivered),
    Hops = Sum(hops),
    log("~nRequests finished in ~.1f s~n", [Secs]),
    log("Requests sent: ~b, answered: ~b, timed out: ~b~n",
              [Sum(sent), Delivered, Sum(timeouts)]),
    log("Stores: ~b, lookups: ~b (correct ~b, wrong ~b, not found ~b)~n",
              [Sum(puts), Sum(gets), Sum(get_ok), Sum(get_wrong), Sum(get_missing)]),
    case D > 0 orelse F > 0 orelse LF > 0 of
        true ->
            Survivors = All -- Killed,
            PS = gather_pastry_stats(Survivors),
            PSum = fun(K) -> lists:sum([maps:get(K, S) || S <- PS]) end,
            log("Transmissions lost: ~b, resends: ~b, reroutes after a dead link: ~b~n",
                [PSum(lost), PSum(retries), PSum(reroutes)]),
            log("Messages dropped: no route ~b, hop limit ~b, direct sends given up ~b~n",
                [PSum(no_route), PSum(hop_limit), PSum(gave_up)]);
        false -> ok
    end,
    log("Expected hops (log16 N): ~.3f~n", [math:log(N) / math:log(16)]),
    case Delivered of
        0 -> io:format("Average number of hops: n/a (no request delivered)~n");
        _ -> io:format("Average number of hops: ~.3f~n", [Hops / Delivered])
    end.

gather_pastry_stats(Pids) ->
    [P ! {'$stats_req', self()} || P <- Pids],
    gather(length(Pids), []).
gather(0, Acc) -> Acc;
gather(K, Acc) ->
    receive {'$stats', _, S} -> gather(K - 1, [S | Acc])
    after 5000 -> Acc
    end.

shuffle(L) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), X} || X <- L])].
