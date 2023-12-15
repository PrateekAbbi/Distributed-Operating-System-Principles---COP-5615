module Chord

open System

#r "nuget: Akka.Remote, 1.5.13"
#r "nuget: Akka, 1.5.13"
#r "nuget: Akka.FSharp, 1.5.13"
#r "nuget: Akka.Serialization.Hyperion, 1.5.13"

open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Collections.Generic
open System.Text

let actorModel =
    ActorSystem.Create(
        "ActorModel",
        ConfigurationFactory.ParseString(
            @"akka {
            stdout-loglevel : ERROR
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }"
        )
    )

type messages =
    | Start_Algo of (int * int)
    | Create of (int * IActorRef)
    | Notify of (int * IActorRef)
    | Stabilize
    | Find_Successor of (int * IActorRef)
    | Found_Successor of (int * IActorRef)
    | Requesting_Predecessor
    | Predecessor_Response of (int * IActorRef)
    | Key_Lookup of (int * int * int)
    | Fix_Fingers
    | Find_Ith_Successor of (int * int * IActorRef)
    | Found_Finger_Entry of (int * int * IActorRef)
    | Start_Lookups of (int)
    | Found_Key of (int)

// let mutable Main_Actor_Ref = null

let mutable total_nodes = 0
let mutable total_requests = 0
let m = 20
let mutable ID_Of_First_Node = 0
let mutable Ref_Of_First_Node = null
let mutable Ref_Of_Second_Node = null
let Stabilizing_Cycle_Time = 100.0
let Fixing_Table_Time = 300.0

let mutable Hash_Size = pown 2 m

let Updating_Element index element list =
    list |> List.mapi (fun i v -> if i = index then element else v)

type fingerEntry(x: int, y: IActorRef) as this =
    let id = x
    let idRef = y
    member this.GetID() = x
    member this.GetRef() = y

let fetchingHash data =
    use sha1Hash = System.Security.Cryptography.SHA1.Create()

    let mutable hash =
        sha1Hash.ComputeHash(Encoding.UTF8.GetBytes(data: string): byte[]) |> bigint

    if hash.Sign = -1 then
        hash <- bigint.Negate(hash)

    hash

let consistentHashFunction (m: double) (numNodes: int) =
    let name = string (numNodes)
    let hash = fetchingHash name
    let x = m |> bigint
    let hashKey = (hash) % x
    let nodeName = hashKey |> string

    nodeName

let printHC (mailbox: Actor<_>) =

    let mutable sum = 0
    let mutable count = 0

    let rec loop () =

        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with
            | Found_Key(hc) ->
                sum <- sum + hc
                count <- count + 1

                if count = total_nodes * total_requests then
                    let avgHC = float (sum) / float (total_nodes * total_requests)

                    printfn "Average HOPCOUNT = %.2f" avgHC
                    mailbox.Context.System.Terminate() |> ignore

            | _ -> ()

            return! loop ()
        }

    loop ()

let mutable refofPrinter = null

let node (id: int) (mailbox: Actor<_>) =

    let mutable first = 0
    let mutable successor = 0
    let mutable refSuccessor = null
    let mutable predecessor = 0
    let mutable refPredecessor = null

    let mutable table = []

    let firstEntry = fingerEntry (0, null)

    let table: fingerEntry[] = Array.create m firstEntry

    let rec loop () =
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with
            | Create(otherID, otherRef) ->
                successor <- otherID
                predecessor <- otherID
                refSuccessor <- otherRef
                refPredecessor <- otherRef

                for i in 0 .. m - 1 do
                    let entry = fingerEntry (successor, refSuccessor)
                    table.[i] <- entry

                actorModel.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.FromSeconds(0.0),
                    TimeSpan.FromMilliseconds(Stabilizing_Cycle_Time),
                    mailbox.Self,
                    Stabilize
                )

                actorModel.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.FromSeconds(0.0),
                    TimeSpan.FromMilliseconds(Stabilizing_Cycle_Time),
                    mailbox.Self,
                    Fix_Fingers
                )

            | Notify(otherId, ref) ->
                predecessor <- otherId
                refPredecessor <- ref

            | Fix_Fingers ->
                let mutable iThFinger = 0

                for i in 1 .. m - 1 do
                    iThFinger <- (id + (pown 2 i)) % int (Hash_Size)
                    mailbox.Self <! Find_Ith_Successor(i, iThFinger, mailbox.Self)

            | Find_Ith_Successor(i, key, ref) ->

                if successor < id && (key > id || key < successor) then
                    ref <! Found_Finger_Entry(i, successor, refSuccessor)
                elif key <= successor && key > id then
                    ref <! Found_Finger_Entry(i, successor, refSuccessor)
                else
                    let mutable Break = false
                    let mutable x = m
                    let mutable temp = key

                    if id > key then
                        temp <- key + Hash_Size

                    while not Break do
                        x <- x - 1

                        if x < 0 then
                            refSuccessor <! Find_Ith_Successor(i, key, ref)
                            Break <- true
                        else
                            let iThFinger = table.[x].GetID()

                            if (iThFinger > id && iThFinger <= temp) then
                                let refOfIthNode = table.[x].GetRef()
                                refOfIthNode <! Find_Ith_Successor(i, key, ref)
                                Break <- true

            | Found_Finger_Entry(i, otherID, ref) ->
                let nodeEntry = fingerEntry (otherID, ref)
                table.[i] <- nodeEntry

            | Stabilize ->
                if successor <> 0 then
                    refSuccessor <! Requesting_Predecessor

            | Predecessor_Response(successorPredecessor, ref) ->

                if successorPredecessor <> id then
                    successor <- successorPredecessor
                    refSuccessor <- ref

                refSuccessor <! Notify(id, mailbox.Self)

            | Requesting_Predecessor -> sender <! Predecessor_Response(predecessor, refPredecessor)

            | Found_Successor(otherID, ref) ->
                successor <- otherID
                refSuccessor <- ref

                for i in 0 .. m - 1 do
                    let nodeEntry = fingerEntry (successor, refSuccessor)
                    table.[i] <- nodeEntry

                actorModel.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(Stabilizing_Cycle_Time),
                    mailbox.Self,
                    Stabilize
                )

                actorModel.Scheduler.ScheduleTellRepeatedly(
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(Fixing_Table_Time),
                    mailbox.Self,
                    Fix_Fingers
                )

                refSuccessor <! Notify(id, mailbox.Self)

            | Key_Lookup(key, hc, by) ->

                if successor < id && (key > id || key <= successor) then
                    refofPrinter <! Found_Key(hc)
                elif key <= successor && key > id then
                    refofPrinter <! Found_Key(hc)
                else
                    let mutable Break = false
                    let mutable x = m
                    let mutable temp = key

                    if id > key then
                        temp <- key + Hash_Size

                    while not Break do
                        x <- x - 1

                        if x < 0 then
                            refSuccessor <! Key_Lookup(key, hc + 1, by)
                            Break <- true
                        else
                            let iThFinger = table.[x].GetID()

                            if (iThFinger > id && iThFinger <= temp) then
                                let refOfIthNode = table.[x].GetRef()
                                refOfIthNode <! Key_Lookup(key, hc + 1, by)
                                Break <- true

            | Start_Lookups(requests) ->
                let mutable temp = 0

                if successor <> ID_Of_First_Node then
                    refSuccessor <! Start_Lookups(requests)

                for x in 1..requests do
                    temp <- Random().Next(1, int (Hash_Size))
                    mailbox.Self <! Key_Lookup(temp, 1, id)
                    System.Threading.Thread.Sleep(800)

            | Find_Successor(otherID, ref) ->

                if successor < id && (otherID > id || otherID < successor) then
                    ref <! Found_Successor(successor, refSuccessor)
                elif otherID <= successor && otherID > id then
                    ref <! Found_Successor(successor, refSuccessor)
                else
                    refSuccessor <! Find_Successor(otherID, ref)

            | _ -> ()

            return! loop ()
        }

    loop ()


let MainActor (mailbox: Actor<_>) =
    let mutable idOfSecondNode = 0
    let mutable idOfTempNode = 0
    let mutable nameOfTempNode = ""
    let mutable refOfTempNode = null
    let mutable temp = 0
    let list = new List<int>()

    let rec loop () =

        actor {
            let! (message) = mailbox.Receive()

            match message with
            | Start_Algo(nodes, requests) ->
                ID_Of_First_Node <- Random().Next(int (Hash_Size))
                Ref_Of_First_Node <- spawn actorModel (sprintf "%d" ID_Of_First_Node) (node ID_Of_First_Node)

                idOfSecondNode <- Random().Next(int (Hash_Size))
                Ref_Of_Second_Node <- spawn actorModel (sprintf "%d" idOfSecondNode) (node idOfSecondNode)

                Ref_Of_First_Node <! Create(idOfSecondNode, Ref_Of_Second_Node)
                Ref_Of_Second_Node <! Create(ID_Of_First_Node, Ref_Of_First_Node)

                for x in 3..nodes do
                    System.Threading.Thread.Sleep(300)

                    idOfTempNode <-
                        [ 1..Hash_Size ]
                        |> List.filter (fun x -> (not (list.Contains(x))))
                        |> fun y -> y.[Random().Next(y.Length - 1)]

                    list.Add(idOfTempNode)

                    refOfTempNode <- spawn actorModel (sprintf "%d" idOfTempNode) (node idOfTempNode)
                    Ref_Of_First_Node <! Find_Successor(idOfTempNode, refOfTempNode)

                System.Threading.Thread.Sleep(8000)
                Ref_Of_First_Node <! Start_Lookups(requests)

            | _ -> ()

            return! loop ()
        }

    loop ()



let startingFun (nodes) (requests) =
    total_nodes <- nodes
    total_requests <- requests

    let Main_Actor_Ref = spawn actorModel "MainActor" MainActor
    refofPrinter <- spawn actorModel "printHC" printHC

    Main_Actor_Ref <! Start_Algo(nodes, requests)
