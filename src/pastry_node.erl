%%% Pastry overlay node. One Erlang process (actor) per peer.
%%%
%%% Parameters follow the Pastry paper: 128-bit ids, b = 4 (hex digits),
%%% leaf set |L| = 16 (8 smaller + 8 larger), neighbourhood set |M| = 16.
%%%
%%% Pastry API (paper section 2.3), as seen by the application (kv_app):
%%%   route(Msg, Key)            - pastry_node:route/2 (from inside a node)
%%%                                pastry_node:route/3 (from any process)
%%%   deliver(Msg, Key)          - kv_app:deliver/5 callback
%%%   forward(Msg, Key, NextId)  - kv_app:forward/3 callback
%%%   newLeafs(LeafSet)          - kv_app:new_leafs/2 callback
%%% The application is a library that runs inside the node process (like the
%%% paper's application layered on a Pastry node); it is not its own actor.
%%%
%%% A node reference is the tuple {Id, Pid, Coord}. Coord is a random point
%%% on a 2^20 x 2^20 grid, packed into one integer as X * 2^20 + Y, and
%%% stands in for network proximity (the paper's "distance metric", e.g.
%%% round-trip time).
%%%
%%% Failure mode (cfg `ft`). Every message between nodes then goes over a
%%% simulated link (transmit/3), which:
%%%   - loses it if the link is permanently broken,
%%%   - loses it with probability `drop` (temporary failure).
%%% Messages are sent with hop-by-hop acknowledgements (rsend/4). A message
%%% that is not acked in time is resent; the wait starts at ?ACK_TIMEOUT ms
%%% and doubles on each try, up to ?LINK_RETRIES transmissions. After that the link is treated as down: routed messages take
%%% another route, other messages are given up and counted.
-module(pastry_node).

-export([start/4, route/2, route/3, send/2]).
-export([init/4, wake/1]).
-export([shl/2, digit/2, ring_dist/2, prox/2]).

-define(DIGITS, 32).            % 128 bits / b(=4)
-define(RING, (1 bsl 128)).
-define(HALF_L, 8).             % |L|/2
-define(M_SIZE, 16).            % |M|
-define(MAX_HOPS, 64).          % messages older than this are dropped
-define(LINK_RETRIES, 3).       % transmissions before a link counts as down
-define(ACK_TIMEOUT, 100).      % ms to wait for the first hop-by-hop ack
-define(SEEN_MAX, 20000).       % size bound of the duplicate-filter
-define(FT_IDLE, 500).          % failure mode: idle ms before hibernating

-record(st, {
    self,                 % {Id, Pid, Coord}
    master,
    lo = [],              % leaf set, smaller half, nearest first
    hi = [],              % leaf set, larger half, nearest first
    rt = #{},             % routing table: {Row, Col} => Ref
    nbr = [],             % neighbourhood set, nearest (proximity) first
    app,                  % application state
    join_acks = 0,        % announce acks still expected while joining
    join_hops = 0,
    %% failure model
    ft = false,           % monitor peers, use acked sends, repair state
    monitored = #{},
    dead = #{},           % Pid => true, peers known to have failed
    broken = #{},         % Pid => true, permanently failed links
    drop = 0.0,           % probability that one transmission is lost
    unacked = #{},        % Tag => {Pid, Msg, Tries, OnFail}
    seen = #{},           % Tags already received (filters resent copies)
    stats = #{lost => 0, retries => 0, reroutes => 0, no_route => 0,
              gave_up => 0, hop_limit => 0}
}).

%%% ------------------------------------------------------------------
%%% API
%%% ------------------------------------------------------------------

start(Id, Coord, Master, Cfg) ->
    %% Full-sweep GCs keep the many small node heaps compact, which matters
    %% for memory and speed once there are tens of thousands of nodes.
    spawn_opt(?MODULE, init, [Id, Coord, Master, Cfg], [{fullsweep_after, 0}]).

%% Pastry route(msg, key), called by the application inside a node process.
%% The node routes the message like any other one, starting with 0 hops.
route(Msg, Key) ->
    route(self(), Msg, Key).

%% Same, but asks node Node to start routing (usable from any process).
route(Node, Msg, Key) ->
    Node ! {'$route', Key, Msg, 0, []},
    ok.

%% Direct node-to-node message from the application (e.g. a replica copy).
%% Goes over the simulated link, acked and resent in failure mode.
send(Pid, Msg) ->
    self() ! {'$send', Pid, Msg},
    ok.

%%% ------------------------------------------------------------------
%%% Actor loop
%%% ------------------------------------------------------------------

init(Id, Coord, Master, Cfg) ->
    St = #st{self = {Id, self(), Coord},
             master = Master,
             ft = maps:get(ft, Cfg, false),
             app = kv_app:init(Id)},
    loop(St).

%% An idle node hibernates: its heap shrinks to the live data, which keeps
%% very large networks within memory. Normally it hibernates as soon as its
%% mailbox is empty. In failure mode it waits ?FT_IDLE ms first, because
%% repair traffic arrives in bursts and hibernating between every message
%% of a burst costs too much CPU.
loop(St) ->
    receive Msg -> loop(handle(Msg, St))
    after idle(St) -> erlang:hibernate(?MODULE, wake, [St])
    end.

idle(#st{ft = true}) -> ?FT_IDLE;
idle(_) -> 0.

wake(St) ->
    receive Msg -> loop(handle(Msg, St)) end.

%% ---- joining (paper section 2.4) ----
%% Joins happen one at a time, before any failure is injected, so join
%% messages use plain sends.

handle({'$start_join', none}, St) ->            % first node of the network
    St#st.master ! {joined, self(), 0},
    St;
handle({'$start_join', Boot}, St) ->
    Boot ! {'$join', St#st.self, 0, []},
    St;

%% A join request for new node X travels towards key X. Each node on the
%% path hands X the routing-table rows 0..i that are valid for X, where i is
%% the prefix it shares with X. (The paper has node i contribute row i only;
%% sending rows 0..i is a superset, so X's table is at least as full.) The
%% first node also hands over its neighbourhood set and the last one (Z,
%% numerically closest to X) its leaf set.
handle({'$join', X = {XId, XPid, _}, Hops, Acc}, St = #st{self = Self = {SId, _, _}}) ->
    I = shl(SId, XId),
    Rows = [R || {{Row, _}, R} <- maps:to_list(St#st.rt), Row =< I],
    Nbr = case Hops of 0 -> St#st.nbr; _ -> [] end,
    Acc1 = [Self | Rows ++ Nbr] ++ Acc,
    case next_hop(XId, St, #{}) of
        {_, {_, NPid, _}} ->
            NPid ! {'$join', X, Hops + 1, Acc1};
        _ ->
            XPid ! {'$join_reply', Acc1 ++ St#st.lo ++ St#st.hi, Hops}
    end,
    St;

%% X builds its state, then tells every node in it that X has arrived.
handle({'$join_reply', Cands, Hops}, St0) ->
    St = insert_many(Cands, St0),
    Known = known_pids(St),
    [P ! {'$announce', St#st.self} || P <- Known],
    St#st{join_acks = length(Known), join_hops = Hops};
handle({'$announce', Ref = {_, P, _}}, St) ->
    P ! '$announce_ack',
    insert(Ref, St);
handle('$announce_ack', St = #st{join_acks = N}) ->
    case N - 1 of
        0 ->
            St#st.master ! {joined, self(), St#st.join_hops},
            erlang:garbage_collect();   % drop the garbage left by the join
        _ -> ok
    end,
    St#st{join_acks = N - 1};

%% ---- routing (paper section 2.2) ----

%% Avoid lists the nodes that already had to detour this message, so a
%% detour never bounces back to them.
handle({'$route', _Key, _Msg, Hops, _Avoid}, St) when Hops > ?MAX_HOPS ->
    count(hop_limit, St);
handle({'$route', Key, Msg, Hops, Avoid}, St) ->
    Excl = lists:foldl(fun(P, E) -> E#{P => true} end, St#st.broken, Avoid),
    route_msg(Key, Msg, Hops, Avoid, St, Excl);

%% ---- simulated links: acked delivery ----

handle({'$rel', Msg, From, Tag}, St) ->
    St1 = transmit(From, {'$ack', Tag}, St),
    case maps:is_key(Tag, St1#st.seen) of
        true -> St1;                                % a resent copy
        false -> handle(Msg, mark_seen(Tag, St1))
    end;
handle({'$ack', Tag}, St) ->
    St#st{unacked = maps:remove(Tag, St#st.unacked)};
handle({'$ack_timeout', Tag}, St) ->
    case maps:find(Tag, St#st.unacked) of
        error ->
            St;                                     % acked in time
        {ok, {Pid, Msg, Tries, OnFail}} when Tries < ?LINK_RETRIES ->
            St1 = transmit(Pid, {'$rel', Msg, self(), Tag}, count(retries, St)),
            erlang:send_after(?ACK_TIMEOUT bsl Tries, self(), {'$ack_timeout', Tag}),
            St1#st{unacked = (St1#st.unacked)#{Tag => {Pid, Msg, Tries + 1, OnFail}}};
        {ok, {Pid, _, _, OnFail}} ->
            %% No ack after all retries: treat the link as down.
            St1 = St#st{unacked = maps:remove(Tag, St#st.unacked)},
            case OnFail of
                {reroute, Key, Msg, Hops, Avoid, Excl} ->
                    route_msg(Key, Msg, Hops, Avoid, count(reroutes, St1),
                              Excl#{Pid => true});
                give_up ->
                    count(gave_up, St1)
            end
    end;
handle({'$send', Pid, Msg}, St) ->
    rsend(Pid, Msg, give_up, St);
handle({'$set_drop', P}, St) ->
    St#st{drop = P};

%% ---- failure detection and repair (paper section 2.4) ----

%% Failures are detected with Erlang process monitors: the runtime tells
%% us when a monitored peer has died.
handle({'DOWN', _, process, Pid, _}, St) ->
    node_down(Pid, St);
handle({'$leafset_req', From}, St) ->
    rsend(From, {'$leafset_resp', [St#st.self | St#st.lo ++ St#st.hi]}, give_up, St);
handle({'$leafset_resp', Refs}, St) ->
    new_leafs(insert_many(Refs, St), St);
handle({'$rt_req', R, C, From}, St) ->
    case maps:find({R, C}, St#st.rt) of
        {ok, Ref} -> rsend(From, {'$rt_resp', Ref}, give_up, St);
        error -> St
    end;
handle({'$rt_resp', Ref}, St) ->
    new_leafs(insert(Ref, St), St);

%% Permanently break a fraction of this node's links (both directions).
%% These two are control messages from the simulation, not network traffic.
handle({'$break_links', Frac}, St) ->
    Chosen = [P || P <- known_pids(St), rand:uniform() < Frac],
    [P ! {'$link_broken', self()} || P <- Chosen],
    St#st{broken = lists:foldl(fun(P, B) -> B#{P => true} end, St#st.broken, Chosen)};
handle({'$link_broken', P}, St) ->
    St#st{broken = (St#st.broken)#{P => true}};

handle({'$stats_req', From}, St) ->
    From ! {'$stats', self(), St#st.stats},
    St;

%% Anything else belongs to the application.
handle(Other, St) ->
    St#st{app = kv_app:handle_info(Other, St#st.app)}.

%%% ------------------------------------------------------------------
%%% Links
%%% ------------------------------------------------------------------

%% One transmission over the simulated link to Pid. It is lost if the link
%% is broken or, with probability `drop`, by a temporary failure.
transmit(Pid, Msg, St = #st{broken = B, drop = D}) ->
    case maps:is_key(Pid, B) orelse (D > 0 andalso rand:uniform() < D) of
        true -> count(lost, St);
        false -> Pid ! Msg, St
    end.

%% Acked send. Outside failure mode this is a plain send.
rsend(Pid, Msg, _OnFail, St = #st{ft = false}) ->
    Pid ! Msg,
    St;
rsend(Pid, Msg, OnFail, St) ->
    Tag = make_ref(),
    St1 = transmit(Pid, {'$rel', Msg, self(), Tag}, St),
    erlang:send_after(?ACK_TIMEOUT, self(), {'$ack_timeout', Tag}),
    St1#st{unacked = (St1#st.unacked)#{Tag => {Pid, Msg, 1, OnFail}}}.

mark_seen(Tag, St = #st{seen = S}) when map_size(S) >= ?SEEN_MAX ->
    St#st{seen = #{Tag => true}};
mark_seen(Tag, St = #st{seen = S}) ->
    St#st{seen = S#{Tag => true}}.

count(K, St = #st{stats = S}) ->
    St#st{stats = S#{K := maps:get(K, S) + 1}}.

%%% ------------------------------------------------------------------
%%% Routing
%%% ------------------------------------------------------------------

%% Excl holds peers that must not be used as next hop for this message
%% (broken links, detouring nodes, or links that just failed).
route_msg(Key, Msg, Hops, Avoid, St, Excl) ->
    case next_hop(Key, St, Excl) of
        self ->
            St#st{app = kv_app:deliver(Msg, Key, Hops, St#st.app, ctx(St))};
        fail ->
            %% No reachable node can take the message any closer. Drop it
            %% rather than deliver it at the wrong node.
            count(no_route, St);
        {Kind, {NextId, NPid, _}} ->
            Avoid1 = case Kind of
                detour -> [self() | Avoid];
                direct -> Avoid
            end,
            case kv_app:forward(Msg, Key, NextId) of
                {ok, Msg1} ->
                    rsend(NPid, {'$route', Key, Msg1, Hops + 1, Avoid1},
                          {reroute, Key, Msg, Hops, Avoid, Excl}, St);
                drop ->
                    St
            end
    end.

%% Returns `self` if this node should deliver Key, {direct, Ref} for a
%% normal Pastry hop, {detour, Ref} when the hop goes around an unreachable
%% destination, or `fail` when no usable node is left.
next_hop(Key, St = #st{self = Self = {SId, _, _}, lo = Lo, hi = Hi}, Excl) ->
    Leaves = Lo ++ Hi,
    case in_leaf_range(Key, Lo, Hi) of
        true ->
            case closest(Key, [Self | Leaves]) of
                Self -> self;
                Best = {_, BP, _} ->
                    case usable(BP, Excl) of
                        true -> {direct, Best};
                        false ->
                            %% Cannot reach the destination directly: hand the
                            %% message to the closest other leaf that we can reach.
                            case [R || R = {_, P, _} <- Leaves, P =/= BP, usable(P, Excl)] of
                                [] -> fail;
                                Cs -> {detour, closest(Key, Cs)}
                            end
                    end
            end;
        false ->
            L = shl(Key, SId),
            case maps:find({L, digit(Key, L)}, St#st.rt) of
                {ok, R = {_, P, _}} ->
                    case usable(P, Excl) of
                        true -> {direct, R};
                        false -> rare_case(Key, L, St, Excl, fail)
                    end;
                error ->
                    rare_case(Key, L, St, Excl, self)
            end
    end.

%% Paper: forward to a node that shares at least as long a prefix with the
%% key as this node, and is numerically closer to the key. If there is none
%% and nothing was excluded, this node is the closest it knows of and
%% delivers (as in the paper); if the right hop was unreachable, fail.
rare_case(Key, L, #st{self = {SId, _, _}, lo = Lo, hi = Hi, rt = Rt, nbr = Nbr}, Excl, None) ->
    D = ring_dist(SId, Key),
    Cs = [R || R = {Id, P, _} <- Lo ++ Hi ++ maps:values(Rt) ++ Nbr,
               usable(P, Excl), shl(Id, Key) >= L, ring_dist(Id, Key) < D],
    case Cs of
        [] -> None;
        _ -> {direct, closest(Key, Cs)}
    end.

in_leaf_range(Key, Lo, Hi) ->
    Small = length(Lo) < ?HALF_L orelse length(Hi) < ?HALF_L,
    Overlap = lists:any(fun({_, P, _}) -> lists:keymember(P, 2, Hi) end, Lo),
    case Small orelse Overlap of
        true -> true;   % the leaf set covers the whole ring
        false ->
            {LoId, _, _} = lists:last(Lo),
            {HiId, _, _} = lists:last(Hi),
            cw(LoId, Key) =< cw(LoId, HiId)
    end.

usable(P, Excl) -> not maps:is_key(P, Excl).

closest(Key, [R | Rs]) ->
    {_, Best} = lists:foldl(
        fun(Ref = {Id, _, _}, {BD, B}) ->
            D = {ring_dist(Id, Key), Id},
            case D < BD of true -> {D, Ref}; false -> {BD, B} end
        end, {{ring_dist(element(1, R), Key), element(1, R)}, R}, Rs),
    Best.

ctx(#st{self = Self, lo = Lo, hi = Hi, ft = Ft}) ->
    #{self => Self, lo => Lo, hi => Hi, ft => Ft}.

%%% ------------------------------------------------------------------
%%% State maintenance
%%% ------------------------------------------------------------------

insert({_, Pid, _}, St) when Pid =:= self() -> St;
insert(Ref = {_, Pid, _}, St) ->
    case maps:is_key(Pid, St#st.dead) of
        true -> St;
        false -> insert_nbr(Ref, insert_rt(Ref, insert_leaf(Ref, watch(Pid, St))))
    end.

%% Inserts many refs at once (join reply, leaf set repair): the leaf set and
%% neighbourhood set are recomputed once instead of once per ref.
insert_many(Refs, St0 = #st{self = Self = {SId, _, _}, lo = Lo, hi = Hi, nbr = Nbr}) ->
    Me = self(),
    New = [R || R = {_, P, _} <- lists:ukeysort(2, Refs),
                P =/= Me, not maps:is_key(P, St0#st.dead)],
    St1 = lists:foldl(fun({_, P, _}, S) -> watch(P, S) end, St0, New),
    Pool = lists:ukeysort(2, New ++ Lo ++ Hi),
    NPool = lists:ukeysort(2, New ++ Nbr),
    St2 = St1#st{lo = nearest(fun({Id, _, _}) -> cw(Id, SId) end, Pool),
                 hi = nearest(fun({Id, _, _}) -> cw(SId, Id) end, Pool),
                 nbr = [R || {_, R} <- lists:sublist(
                                lists:sort([{prox(Self, R), R} || R <- NPool]), ?M_SIZE)]},
    lists:foldl(fun insert_rt/2, St2, New).

%% Failure mode only: monitor each peer we learn about, once.
watch(Pid, St = #st{ft = true, monitored = M}) ->
    case maps:is_key(Pid, M) of
        true -> St;
        false ->
            erlang:monitor(process, Pid),
            St#st{monitored = M#{Pid => true}}
    end;
watch(_, St) -> St.

insert_leaf(Ref = {_, Pid, _}, St = #st{self = {SId, _, _}, lo = Lo, hi = Hi}) ->
    case lists:keymember(Pid, 2, Lo) orelse lists:keymember(Pid, 2, Hi) of
        true -> St;
        false ->
            Pool = lists:ukeysort(2, [Ref | Lo ++ Hi]),
            St#st{lo = nearest(fun({Id, _, _}) -> cw(Id, SId) end, Pool),
                  hi = nearest(fun({Id, _, _}) -> cw(SId, Id) end, Pool)}
    end.

nearest(DistFun, Pool) ->
    Sorted = lists:sort([{DistFun(R), R} || R <- Pool]),
    [R || {_, R} <- lists:sublist(Sorted, ?HALF_L)].

insert_rt(Ref = {Id, Pid, _}, St = #st{self = Self = {SId, _, _}, rt = Rt}) ->
    Row = shl(SId, Id),
    Slot = {Row, digit(Id, Row)},
    case maps:find(Slot, Rt) of
        error -> St#st{rt = Rt#{Slot => Ref}};
        {ok, {_, Pid, _}} -> St;
        {ok, Old} ->
            %% Prefer the entry that is closer in the proximity space.
            case prox(Self, Ref) < prox(Self, Old) of
                true -> St#st{rt = Rt#{Slot => Ref}};
                false -> St
            end
    end.

insert_nbr(Ref = {_, Pid, _}, St = #st{self = Self, nbr = Nbr}) ->
    case lists:keymember(Pid, 2, Nbr) of
        true -> St;
        false ->
            Sorted = lists:sort([{prox(Self, R), R} || R <- [Ref | Nbr]]),
            St#st{nbr = [R || {_, R} <- lists:sublist(Sorted, ?M_SIZE)]}
    end.

known_pids(#st{lo = Lo, hi = Hi, rt = Rt, nbr = Nbr}) ->
    lists:usort([P || {_, P, _} <- Lo ++ Hi ++ maps:values(Rt) ++ Nbr]).

%% A peer failed: drop it everywhere and repair the leaf set and routing
%% table by asking live peers, as described in the paper.
node_down(Pid, St = #st{lo = Lo, hi = Hi, rt = Rt, nbr = Nbr}) ->
    WasLeaf = lists:keymember(Pid, 2, Lo) orelse lists:keymember(Pid, 2, Hi),
    Keep = fun(Refs) -> [R || R = {_, P, _} <- Refs, P =/= Pid] end,
    Lost = [Slot || {Slot, {_, P, _}} <- maps:to_list(Rt), P =:= Pid],
    Rt1 = maps:without(Lost, Rt),
    St1 = St#st{dead = (St#st.dead)#{Pid => true},
                lo = Keep(Lo), hi = Keep(Hi), nbr = Keep(Nbr), rt = Rt1},
    St2 = case WasLeaf of
        true ->
            %% Refill from what we already know, then ask the farthest live
            %% leaf on each side for its leaf set.
            S0 = refill_leaves(St1),
            S1 = lists:foldl(fun({_, P, _}, S) ->
                                 rsend(P, {'$leafset_req', self()}, give_up, S)
                             end, S0, far_ends(S0#st.lo, S0#st.hi)),
            new_leafs(S1, St);
        false ->
            St1
    end,
    %% Ask another entry of the same row for a replacement entry (paper:
    %% R[l][d] is asked for its own R[l][d]).
    St3 = lists:foldl(
        fun({R, C}, S) ->
            case [P || {{R2, _}, {_, P, _}} <- maps:to_list(S#st.rt), R2 =:= R] of
                [] -> S;
                Peers ->
                    P = lists:nth(rand:uniform(length(Peers)), Peers),
                    rsend(P, {'$rt_req', R, C, self()}, give_up, S)
            end
        end, St2, Lost),
    %% Messages still waiting for an ack from the dead peer: reroute routed
    %% ones now (the tables no longer contain it), give up on the rest.
    {ToDead, Rest} = lists:partition(fun({_, {P, _, _, _}}) -> P =:= Pid end,
                                     maps:to_list(St3#st.unacked)),
    lists:foldl(
        fun({_, {_, _, _, {reroute, Key, Msg, Hops, Avoid, Excl}}}, S) ->
                route_msg(Key, Msg, Hops, Avoid, count(reroutes, S), Excl#{Pid => true});
           ({_, {_, _, _, give_up}}, S) ->
                count(gave_up, S)
        end, St3#st{unacked = maps:from_list(Rest)}, ToDead).

%% Recomputes the leaf set from every node we still know of.
refill_leaves(St = #st{self = {SId, _, _}, lo = Lo, hi = Hi, rt = Rt, nbr = Nbr}) ->
    Pool = lists:ukeysort(2, Lo ++ Hi ++ maps:values(Rt) ++ Nbr),
    St#st{lo = nearest(fun({Id, _, _}) -> cw(Id, SId) end, Pool),
          hi = nearest(fun({Id, _, _}) -> cw(SId, Id) end, Pool)}.

far_ends(Lo, Hi) ->
    lists:usort([lists:last(L) || L <- [Lo, Hi], L =/= []]).

%% newLeafs(leafSet) upcall when the leaf set changed.
new_leafs(New = #st{lo = Lo, hi = Hi}, #st{lo = Lo, hi = Hi}) -> New;
new_leafs(New, #st{lo = OldLo, hi = OldHi}) ->
    Ctx = (ctx(New))#{old => OldLo ++ OldHi},
    New#st{app = kv_app:new_leafs(Ctx, New#st.app)}.

%%% ------------------------------------------------------------------
%%% Id arithmetic
%%% ------------------------------------------------------------------

%% Length of the shared prefix of two ids, in hex digits.
shl(A, B) ->
    case A bxor B of
        0 -> ?DIGITS;
        X -> lz(X, 0)
    end.
lz(X, I) ->
    case digit(X, I) of
        0 -> lz(X, I + 1);
        _ -> I
    end.

digit(Id, I) -> (Id bsr (4 * (?DIGITS - 1 - I))) band 15.

%% Clockwise distance from A to B on the id ring.
cw(A, B) -> ((B - A) rem ?RING + ?RING) rem ?RING.

ring_dist(A, B) ->
    D = cw(A, B),
    min(D, ?RING - D).

%% Proximity (squared distance in the plane; only used for comparisons).
prox({_, _, C1}, {_, _, C2}) ->
    DX = (C1 bsr 20) - (C2 bsr 20),
    DY = (C1 band 16#FFFFF) - (C2 band 16#FFFFF),
    DX * DX + DY * DY.
