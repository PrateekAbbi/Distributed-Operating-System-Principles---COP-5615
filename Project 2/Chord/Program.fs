module Program

open Chord
open System

[<EntryPoint>]
let main argv =
    let nodes = argv.[0] |> int
    let requests = argv.[1] |> int

    let actorModel = Chord.actorModel

    Chord.startingFun nodes requests

    actorModel.WhenTerminated.Wait()

    0
