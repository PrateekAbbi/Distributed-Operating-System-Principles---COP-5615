module Client

open System
open System.Net.Sockets
open System.Text

let rec clientMain (port: int) =
    try
        let client = new TcpClient("127.0.0.1", port)
        let stream = client.GetStream()

        printfn "Enter your command and numbers (e.g., add 1 2 3) or 'bye/terminal' to quit:"
        let user_input = Console.ReadLine()

        if user_input = "bye" then
            printfn "Sending command: %s" user_input
            printfn "Exit"
        elif user_input = "terminate" then
            printfn "Sending command: %s" user_input
            let message = Encoding.ASCII.GetBytes(user_input)
            stream.Write(message, 0, message.Length)
        else
            printfn "Sending command: %s" user_input
            let message = Encoding.ASCII.GetBytes(user_input)
            stream.Write(message, 0, message.Length)

            let buffer = Array.zeroCreate 256
            let bytesRead = stream.Read(buffer, 0, buffer.Length)
            let response = Encoding.ASCII.GetString(buffer, 0, bytesRead).Trim() |> int

            if response = -1 then
                printfn "Server response: Incorrect Operation Command"
            elif response = -2 then
                printfn "Server response: Number of Inputs is less than 2"
            elif response = -3 then
                printfn "Server response: Numbers of Input is more than 4"
            elif response = -4 then
                printfn "One or more of the inputs contain(s) non-number(s)"
            else
                printfn "Server response: %s" (response.ToString())

            clientMain port // Recursive call to continue

        client.Close()
    with ex ->
        if ex.Message <> "Input string was not in a correct format." then
            printfn "An error occurred: %s" (ex.Message)
