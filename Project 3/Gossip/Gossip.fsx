#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

// hyperparameters
let actor_system =
    ActorSystem.Create(
        "Gossip",
        ConfigurationFactory.ParseString(
            @"akka {
            stdout-loglevel : ERROR
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }"
        )
    )

let magical_number = 10
let small_change: double = 10.0 ** -10.0
let convergence_rounds = 5
let push_sum_goal: string = "avg"
let gossip_interval = 300

type boss_message =
    | Start_Gossip of int * string * int
    | Received_Gossip of int
    | Gossiping of string
    | Finished_Gossip of int * int
    | Start_Push of int * string
    | Pushing of bool
    | Finished_Push of float

type node_message =
    | Init of int[] * int * string * bool
    | Gossip of string
    | Triggering_Gossip of string
    | Push of double * double
    | Triggering_Push of bool

let construct_full_network (network: byref<_>, num_nodes: int) =
    for i = 0 to num_nodes - 1 do
        let mutable arr: int[] = Array.empty

        for j = 0 to num_nodes - 1 do
            if i <> j then
                arr <- Array.append arr [| j |]

        network <- Array.append network [| arr |]

let construct_2D_network (network: byref<_>, num_nodes: int) =
    let sqrt_num_nodes = int (ceil (sqrt (float (num_nodes))))

    for i = 0 to num_nodes - 1 do
        let mutable arr: int[] = Array.empty

        if (i - sqrt_num_nodes) >= 0 then
            arr <- Array.append arr [| i - sqrt_num_nodes |]

        if (i + sqrt_num_nodes) <= num_nodes - 1 then
            arr <- Array.append arr [| i + sqrt_num_nodes |]

        if (i % sqrt_num_nodes) <> 0 then
            arr <- Array.append arr [| i - 1 |]

        if (((i + 1) % sqrt_num_nodes) <> 0) && ((i + 1) < num_nodes) then
            arr <- Array.append arr [| i + 1 |]

        network <- Array.append network [| arr |]

let construct_line_network (network: byref<_>, num_nodes: int) =
    for i = 0 to num_nodes - 1 do
        if i = 0 then
            network <- Array.append network [| [| i + 1 |] |]
        elif i > 0 && i < num_nodes - 1 then
            network <- Array.append network [| [| i - 1; i + 1 |] |]
        elif i = num_nodes - 1 then
            network <- Array.append network [| [| i - 1 |] |]

let construct_imperfect_3D_network (network: byref<_>, num_nodes: int) =
    let side = int (Math.Ceiling(Math.Pow(float num_nodes, 1.0 / 3.0)))

    for i = 0 to num_nodes - 1 do
        let mutable arr: int[] = Array.empty

        let x = i / (side * side)
        let y = (i / side) % side
        let z = i % side

        // Add neighbors in 3D grid
        for dx in -1 .. 1 do
            for dy in -1 .. 1 do
                for dz in -1 .. 1 do
                    if dx <> 0 || dy <> 0 || dz <> 0 then
                        let nx = x + dx
                        let ny = y + dy
                        let nz = z + dz

                        if nx >= 0 && nx < side && ny >= 0 && ny < side && nz >= 0 && nz < side then
                            let neighborIndex = nx * side * side + ny * side + nz
                            arr <- Array.append arr [| neighborIndex |]

        // Add one random additional neighbor
        let random = new Random()
        let randomNeighbor = random.Next(num_nodes)

        if not (Array.contains randomNeighbor arr) then
            arr <- Array.append arr [| randomNeighbor |]

        network <- Array.append network [| arr |]



let node (node_mailbox: Actor<node_message>) =
    //Gossip init
    let mutable index_of_node: int = -1
    let mutable count: int = 0
    let mutable neighbouring_arr: int[] = Array.empty

    //Push-sum init
    let mutable initial_sum: double = 0.0
    let mutable initial_weight: double = 1.0
    let mutable current_ratio: double = 0.0
    let mutable count_push: int = 1
    let mutable does_converge: bool = false

    //For both
    let boss_actor = select ("akka://Gossip/user/boss_gossip") actor_system
    let random = new System.Random()
    let mutable does_fail: bool = false

    let rec loop () =
        actor {
            let! (msg: node_message) = node_mailbox.Receive()

            match msg with
            | Init(neighbors, index, type_of_push, failure) ->
                neighbouring_arr <- Array.append neighbouring_arr neighbors
                index_of_node <- index
                initial_sum <- double (index)

                if failure then
                    does_fail <- true

                if type_of_push = "sum" then
                    if index <> 0 then
                        initial_weight <- 0.0
            | Gossip mssg_gossip ->
                if does_fail then
                    count <- magical_number

                if count < magical_number then
                    let random_neighbour: int = random.Next(neighbouring_arr.Length)
                    let next_neighbour: int = neighbouring_arr.[random_neighbour]

                    let actor_of_neighbour =
                        select ("akka://Gossip/user/node" + string next_neighbour) actor_system
                    // tell the boss that this node can start gossiping periodically
                    if count = 0 then
                        boss_actor <! Received_Gossip index_of_node

                    count <- count + 1
                    // printfn "%d %d" index_of_node count
                    actor_of_neighbour <! Gossip mssg_gossip
                else if count = magical_number then
                    boss_actor <! Finished_Gossip(index_of_node, count)

            | Triggering_Gossip mssg_gossip ->
                if count < magical_number then
                    let random_neighbour: int = random.Next(neighbouring_arr.Length)
                    let next_neighbour: int = neighbouring_arr.[random_neighbour]

                    let actor_of_neighbour =
                        select ("akka://Gossip/user/node" + string next_neighbour) actor_system

                    actor_of_neighbour <! Gossip mssg_gossip

            | Push(sum, weight) ->
                if not does_converge then
                    initial_sum <- double (initial_sum + sum)
                    initial_weight <- double (initial_weight + weight)
            // printfn "%.10f %.10f" s w
            | Triggering_Push push ->
                if does_fail then
                    does_converge <- true

                if not does_converge then
                    let mutable ratio: double = double (initial_sum / initial_weight)
                    let diff: double = abs (double (current_ratio) - double (ratio))
                    // printfn "%.10f" diff
                    if diff <= small_change then
                        //printfn "%.10f" x
                        count_push <- count_push + 1

                        if count_push = convergence_rounds then
                            // printfn "Done: %d" index_of_node
                            does_converge <- true
                            boss_actor <! Finished_Push ratio
                    else
                        count_push <- 0

                    current_ratio <- ratio
                    let random_neighbour: int = System.Random().Next(neighbouring_arr.Length)
                    let next_neighbour: int = neighbouring_arr.[random_neighbour]

                    let actor_of_neighbour =
                        select ("akka://Gossip/user/node" + string next_neighbour) actor_system

                    actor_of_neighbour
                    <! Push(double (initial_sum * 0.5), double (initial_weight * 0.5))

                    node_mailbox.Self.Tell(Push(double (initial_sum * 0.5), double (initial_weight * 0.5)))
                    initial_sum <- 0.0
                    initial_weight <- 0.0
            // s <- double(s * 0.5)
            // w <- double(w * 0.5)

            return! loop ()
        }

    loop ()

let boss_gossip (boss_mailbox: Actor<boss_message>) =
    //things to keep track of as a boss
    let mutable node_array_actor: int[] = Array.empty
    let mutable num_nodes: int = 0
    let mutable count: int = 0

    // boss timer
    let timer_of_boss = new System.Diagnostics.Stopwatch()

    let rec loop () =
        actor {
            let! (msg: boss_message) = boss_mailbox.Receive()

            match msg with
            | Start_Gossip(index_of_node, mssg_gossip, number) ->
                let actor_node =
                    select ("akka://Gossip/user/node" + string index_of_node) actor_system

                num_nodes <- number
                timer_of_boss.Start()
                actor_node <! Gossip "Fire!"
            | Received_Gossip index -> node_array_actor <- Array.append node_array_actor [| index |]
            | Gossiping mssg_gossip ->
                if node_array_actor.Length <> 0 then
                    for index in node_array_actor do
                        let actor_node = select ("akka://Gossip/user/node" + string index) actor_system
                        actor_node <! Triggering_Gossip mssg_gossip
            | Finished_Gossip(index, msgNum) ->
                node_array_actor <- Array.filter ((<>) index) node_array_actor
                count <- count + 1

                if msgNum <> magical_number then
                    printfn ("Something funny is going on")

                if count = num_nodes then
                    printfn "Finished gossiping. The time taken: %i" timer_of_boss.ElapsedMilliseconds
                    Environment.Exit 1
            | Start_Push(number, type_of_push) ->
                num_nodes <- number
                timer_of_boss.Start()

                for i = 0 to number - 1 do
                    let actor_node = select ("akka://Gossip/user/node" + string i) actor_system

                    if type_of_push = "avg" then
                        actor_node <! Push(double (i), 1.0)
                    else if type_of_push = "sum" then
                        if i = 0 then
                            actor_node <! Push(double (i), 1.0)
                        else
                            actor_node <! Push(double (i), 0.0)
            | Pushing push ->
                for i = 0 to num_nodes - 1 do
                    let actor_node = select ("akka://Gossip/user/node" + string i) actor_system
                    actor_node <! Triggering_Push true
            | Finished_Push ratio ->
                count <- count + 1
                // printfn "%.10f" ratio

                if count = num_nodes then
                    printfn "Finished push-sum. The time taken: %i" timer_of_boss.ElapsedMilliseconds
                    Environment.Exit 1

            return! loop ()
        }

    loop ()

let main (nodes) (topology) (algorithm) =
    // command line args
    try
        // let args = fsi.CommandLineArgs
        let num_nodes: int = nodes
        let topology: string = topology
        let algorithm: string = algorithm
        // let fRate : int = int args.[4]

        // central info
        let timer = new System.Diagnostics.Stopwatch()
        let mutable network: int[][] = Array.empty
        let boss_gossip = spawn actor_system "boss_gossip" boss_gossip

        let array_node_actor =
            Array.init num_nodes (fun index -> spawn actor_system ("node" + string index) node)

        // start the timer and record the first timestamp
        timer.Start()
        let mutable last_time_stamp = timer.ElapsedMilliseconds

        // build the topology
        match topology with
        | "full" -> construct_full_network (&network, num_nodes)
        | "2D" -> construct_2D_network (&network, num_nodes)
        | "line" -> construct_line_network (&network, num_nodes)
        | "imp3D" -> construct_imperfect_3D_network (&network, num_nodes)
        | _ ->
            printfn "Topology not recognized"
            Environment.Exit 1

        printfn "Topology successfully built!! The time taken: %i" timer.ElapsedMilliseconds

        for i = 0 to num_nodes - 1 do
            array_node_actor.[i] <! Init(network.[i], i, push_sum_goal, false)

        match algorithm with
        | "gossip" ->
            boss_gossip <! Start_Gossip(0, "Fire!", num_nodes)
            // simulate participants who know the message gossiping
            last_time_stamp <- timer.ElapsedMilliseconds

            while true do
                if (timer.ElapsedMilliseconds - last_time_stamp) >= int64 (gossip_interval) then
                    last_time_stamp <- timer.ElapsedMilliseconds
                    boss_gossip <! Gossiping "Fire!"

        | "push-sum" ->
            boss_gossip <! Start_Push(num_nodes, push_sum_goal)
            last_time_stamp <- timer.ElapsedMilliseconds

            while true do
                if (timer.ElapsedMilliseconds - last_time_stamp) >= int64 (gossip_interval) then
                    last_time_stamp <- timer.ElapsedMilliseconds
                    boss_gossip <! Pushing true
        | _ ->
            printfn "Algorithm not recognized"
            Environment.Exit 1
    with :? IndexOutOfRangeException ->
        printfn "Wrong number of input arguments! Please refer to the following format:"
        printfn "dotnet fsi --langversion:preview proj2.fsx [NUMBER OF NODES] [TOPOLOGY] [ALGORITHM]"

    0

// main ()
