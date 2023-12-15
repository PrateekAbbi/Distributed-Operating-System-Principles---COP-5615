module Program

open Gossip

[<EntryPoint>]
let main argv =
    let nodes = argv.[0] |> int
    let topoplogy = argv.[1]
    let algorithm = argv.[2]

    let actorModel = actor_system

    main nodes topoplogy algorithm |> ignore

    actorModel.WhenTerminated.Wait()

    0
