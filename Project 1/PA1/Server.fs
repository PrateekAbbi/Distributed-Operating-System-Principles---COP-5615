module Server

open System
open System.Net
open System.Net.Sockets
open System.Text

let mutable clients = 0

let operations (command: string) (numbers: string list) =
    try
        let numbers = List.map int numbers

        if (List.length numbers < 2) then
            -2
        elif (List.length numbers > 4) then
            -3
        elif (command <> "add" && command <> "subtract" && command <> "multiply") then
            -1
        else
            match command.ToLower() with
            | "add" -> List.sum numbers
            | "subtract" -> List.reduce (-) numbers
            | "multiply" -> List.reduce (*) numbers
            | "terminate" -> -5
            | "bye" -> -5
            | _ -> -4
    with _ ->
        -4

let serverMain (port: int) =
    try
        let listener = new TcpListener(IPAddress.Any, port)
        listener.Start()
        printfn "Server is running and listening on port %d." port

        while true do
            let client = listener.AcceptTcpClient()
            clients <- clients + 1
            let stream = client.GetStream()
            let buffer = Array.zeroCreate 256
            let bytesRead = stream.Read(buffer, 0, buffer.Length)
            let message = Encoding.ASCII.GetString(buffer, 0, bytesRead).Trim()
            printfn "Received: %s" message

            let parts = message.Split(' ') |> Array.toList
            let command = List.head parts
            let numbers = List.tail parts

            if command = "terminate" then
                printfn "Responding to client %d with result: %d" clients -5
                client.Close()
                listener.Stop()
            elif command = "bye" then
                client.Close()
            else
                match operations command numbers with
                | result ->
                    printfn "Responding to client %d with result: %A" clients result
                    let response = Encoding.ASCII.GetBytes(result.ToString())
                    stream.Write(response, 0, response.Length)
    // client.Close()
    with ex ->
        if
            ex.Message
            <> "Not listening. You must call the Start() method before calling this method."
        then
            printfn "An error occurred: %s" (ex.Message)
