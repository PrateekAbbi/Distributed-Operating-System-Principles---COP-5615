open System

// Import the Server and Client modules
open Server
open Client

[<EntryPoint>]
let main argv =
    printfn "Enter the port number:"
    let port = Int32.Parse(Console.ReadLine())
    printfn "Do you want to start the server or the client? (Type 'server' or 'client')"

    match Console.ReadLine() with
    | "server" ->
        serverMain port
        0
    | "client" ->
        clientMain port
        0
    | _ ->
        printfn "Invalid option. Exiting."
        0
