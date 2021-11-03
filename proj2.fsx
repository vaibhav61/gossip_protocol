#if INTERACTIVE
#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
#endif

open System
open Akka.Actor 
open Akka.Configuration
open Akka.FSharp
open System.Collections.Generic


type Gossip =
    |RecordNumberOfPeople of int
    |InitialiseNeighbors of IActorRef[]
    |CaptureStartTime of int * IActorRef[]
    |GossipStart of String
    |CalculateResultForPushSum of Double * Double
    |RecordMessageReceived of String
    |PushSumStart 
    |PushSumCalculate of Double * Double 


let random = System.Random(1)

let Listener (mailbox:Actor<_>) = 

    let mutable nodes:IActorRef[] = [||]
    let mutable messageReceived = 0
    let mutable startTime = 0
    let mutable numberOfPeople =0 

    let rec loop() = actor {
            let! memo = mailbox.Receive()
            match memo with 
            |RecordMessageReceived memo ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                messageReceived <- messageReceived + 1
                if messageReceived = numberOfPeople then 
                    let timeTaken = abs(endTime - startTime)
                    printfn "Convergence Time ::  %i ms" timeTaken
                    Environment.Exit 0
                else
                    //printfn "numofpeople = %d" numberOfPeople
                    let pointer= System.Random().Next(0,nodes.Length)
                    nodes.[pointer] <! GossipStart("Hi") 
          
            |CalculateResultForPushSum (sum,weight) ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                printfn "Sum = %f Weight= %f Average=%f" sum weight (sum/weight) 
                let timeTaken = abs(endTime - startTime)
                printfn "Time for convergence: %i ms" timeTaken
                Environment.Exit 0
          
            |CaptureStartTime (beginTime, nodesReference) ->
                startTime <-beginTime
                nodes <- nodesReference

            |RecordNumberOfPeople numPeople ->
                numberOfPeople <- numPeople

            | _->
                printfn "Invalid Message"
                Environment.Exit 1
            return! loop()
        }
    loop()


  
let Node listener nodeNumber (mailbox:Actor<_>) =
    let mutable numOfMessagesHeard = 0
    let mutable neighbors:IActorRef[] =  [||]

    let mutable tempSum = nodeNumber |> float
    let mutable weight = 1.0
    let mutable turnRound = 1

    let rec loop() = actor {
        let! memo = mailbox.Receive()
        match memo with 
        |InitialiseNeighbors aref ->
            neighbors <- aref 

        |GossipStart msg -> 
            numOfMessagesHeard <- numOfMessagesHeard + 1
            if(numOfMessagesHeard >= 10) then 
                listener <! RecordMessageReceived(msg)
            else
                let pointer = System.Random().Next(0, neighbors.Length)
                neighbors.[pointer] <! GossipStart(msg)


        |PushSumStart  ->
            let pointer = System.Random().Next(0, neighbors.Length)
            tempSum <- tempSum/2.0
            weight <- weight/2.0
            neighbors.[pointer] <! PushSumCalculate(tempSum, weight)

        |PushSumCalculate(s: float, w) ->
            let lambda = 10.0 ** -10.0
            let nSum = tempSum + s
            let nWeight = weight + w
            let computePushSum = tempSum/weight - nSum/nWeight |> abs

            if(computePushSum > lambda) then 
                turnRound <- 0
                tempSum <- tempSum + s 
                weight <- weight + w
                tempSum <- tempSum/2.0 
                weight <- weight/2.0
                let pointer = System.Random().Next(0, neighbors.Length) 
                neighbors.[pointer] <! PushSumCalculate(tempSum, weight)
            elif (turnRound >= 3) then 
                listener<! CalculateResultForPushSum(tempSum, weight)
            else 
                tempSum <- tempSum/2.0
                weight <- weight/2.0
                turnRound <- turnRound + 1
                let pointer = System.Random().Next(0, neighbors.Length)
                neighbors.[pointer] <! PushSumCalculate(tempSum, weight)

        |_-> ()

        return! loop()
    }
    loop() 


let system = ActorSystem.Create("System")
let args : string array = fsi.CommandLineArgs |> Array.tail
let mutable numNodes = args.[0] |> int
let topology = args.[1]
let algorithm = args.[2]
let mutable gridSize = 0 


if (topology = "2D" || topology = "imp2D") then 
    gridSize <- floor (sqrt(float numNodes)) |> int
    numNodes <- gridSize * gridSize   
  

let listener =
    Listener    
    |> spawn system "listener"



match topology with  
    |"line" ->
        let arrayNode = Array.zeroCreate (numNodes)
        let mutable neighborArray:IActorRef[] = [||]
        for i in [0..numNodes-1] do
            arrayNode.[i] <- Node listener (i + 1)
                            |> spawn system ("Node" + string(i))
        for i in [0..numNodes-1] do
            if (i = 0) then 
                neighborArray <- [|arrayNode.[i + 1]|]
            elif (i = numNodes - 1) then 
                neighborArray <- [|arrayNode.[i - 1]|] 
            else 
                neighborArray <- [|arrayNode.[i + 1]; arrayNode.[i - 1]|] 
            arrayNode.[i] <! InitialiseNeighbors(neighborArray) 


        let startNode = System.Random().Next(0, numNodes - 1)
    
        if algorithm = "gossip" then 
            listener <! RecordNumberOfPeople(numNodes)
            listener <! CaptureStartTime(System.DateTime.Now.TimeOfDay.Milliseconds, arrayNode)
            printfn "Starting Gossip for Line..."
            arrayNode.[startNode] <! GossipStart("Line Topology")
        else if algorithm = "push-sum" then  
            listener <! CaptureStartTime(System.DateTime.Now.TimeOfDay.Milliseconds, arrayNode)
            printfn "Starting Push-Sum for line..."
            arrayNode.[startNode] <! PushSumStart

    |"2D" -> 
        let arrayNode = Array.zeroCreate(numNodes)
        for i in [0..numNodes-1] do
            arrayNode.[i] <- Node listener (i + 1)
                            |> spawn system ("Node" + string(i))

        for i in [0.. numNodes-1] do
            let mutable neighborArray : IActorRef[] = [||] 
            
            //top
            if i - gridSize >= 0 then   
                neighborArray <- Array.append neighborArray[|arrayNode.[i - gridSize]|]   

            //bottom
            if i + gridSize < gridSize * gridSize then
                neighborArray <- Array.append neighborArray[|arrayNode.[i + gridSize]|]  

            //left
            if i % gridSize > 0 then 
                neighborArray <- Array.append neighborArray[|arrayNode.[i - 1]|]  

            //right
            if (i + 1) % gridSize > 0 then 
                neighborArray <- Array.append neighborArray[|arrayNode.[i + 1]|]  
            
            arrayNode.[i] <! InitialiseNeighbors(neighborArray)

        let startNode = System.Random().Next(0, numNodes - 1)

        if algorithm = "gossip" then
          listener<!RecordNumberOfPeople(numNodes)
          listener<!CaptureStartTime(System.DateTime.Now.TimeOfDay.Milliseconds, arrayNode)
          printfn "Starting Gossip for 2D..."
          arrayNode.[startNode]<!GossipStart("Hi")

        else if algorithm ="push-sum" then
           listener<!CaptureStartTime(System.DateTime.Now.TimeOfDay.Milliseconds, arrayNode)
           printfn "Starting Push-Sum for 2D..."
           arrayNode.[startNode]<!PushSumStart
           
    |"imp2D" ->
        let arrayNode = Array.zeroCreate(numNodes)
        for i in [0..numNodes-1] do
            arrayNode.[i] <- Node listener (i + 1)
                            |> spawn system ("Node" + string(i))

        for i in [0.. numNodes-1] do
            let mutable neighborArray : IActorRef[] = [||]
            let listNeighbor = new List<int>() 
            
            //top
            if i - gridSize >= 0 then   
                neighborArray <- Array.append neighborArray[|arrayNode.[i - gridSize]|]   
                listNeighbor.Add(i - gridSize)

            //bottom
            if i + gridSize < gridSize * gridSize then
                neighborArray <- Array.append neighborArray[|arrayNode.[i + gridSize]|]  
                listNeighbor.Add(i + gridSize)
                
            //left
            if i % gridSize > 0 then 
                neighborArray <- Array.append neighborArray[|arrayNode.[i - 1]|]  
                listNeighbor.Add(i - 1)

            //right
            if (i + 1) % gridSize > 0 then 
                neighborArray <- Array.append neighborArray[|arrayNode.[i + 1]|]  
                listNeighbor.Add(i + 1)


            let mutable randomNeighbor = System.Random().Next(0, numNodes - 1)
            while listNeighbor.Contains(randomNeighbor) do
                randomNeighbor <- System.Random().Next(0, numNodes - 1) 

            neighborArray <- Array.append neighborArray[|arrayNode.[randomNeighbor]|]    
            arrayNode.[i] <! InitialiseNeighbors(neighborArray)

        let startNode = System.Random().Next(0, numNodes - 1)
        if algorithm = "gossip" then
          listener<!RecordNumberOfPeople(numNodes)
          listener<!CaptureStartTime(System.DateTime.Now.TimeOfDay.Milliseconds, arrayNode)
          printfn "Starting Gossip for imp2D..."
          arrayNode.[startNode] <! GossipStart("Hi")
        else if algorithm ="push-sum" then
           listener<!CaptureStartTime(System.DateTime.Now.TimeOfDay.Milliseconds, arrayNode)
           printfn "Starting Push-Sum for imp2D..."
           arrayNode.[startNode] <! PushSumStart  

    |"full" ->
        let arrayNode = Array.zeroCreate(numNodes)
        //let mutable neighborArray = Array.zeroCreate()
        for i in [0..numNodes-1] do
            arrayNode.[i] <- Node listener (i+1)
                            |> spawn system ("Node" + string(i)) 

        for i in [0..numNodes-1] do
             arrayNode.[i] <! InitialiseNeighbors(arrayNode)
        
        let startNode = System.Random().Next(0,numNodes)
        if algorithm="gossip" then 
           listener<!RecordNumberOfPeople(numNodes)
           listener<!CaptureStartTime(System.DateTime.Now.TimeOfDay.Milliseconds, arrayNode)
           printfn "Starting Gossip for full..."
           arrayNode.[startNode] <! GossipStart("Hi")
        else if algorithm="push-sum" then
           listener<!CaptureStartTime(System.DateTime.Now.TimeOfDay.Milliseconds, arrayNode)
           printfn "Starting Push-Sum for full..."
           arrayNode.[startNode] <! PushSumStart 
    | _-> () 
System.Console.ReadLine() |> ignore