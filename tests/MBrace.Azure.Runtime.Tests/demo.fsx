#I "../../bin/"
#r "Vagrant.dll"
#r "MBrace.Core.dll"
#r "MBrace.Library.dll"
#r "MBrace.Azure.Runtime.Common.dll"
#r "MBrace.Azure.Runtime.dll"
#r "MBrace.Azure.Client.dll"

open Nessos.MBrace
open Nessos.MBrace.Azure.Runtime
open Nessos.MBrace.Azure.Client


// Create your azure specific configuration.
let config = 
    { Configuration.Default with
        StorageConnectionString    = "[connection string]"
        ServiceBusConnectionString = "[connection string]"
    }

// Wait until at least one worker is active, 
// get a handle for this runtime.
let client = Runtime.GetHandle(config, waitWorkerCount = 1)

client.ShowWorkers()


let sayHello = cloud { return "Hello world" }

client.Run sayHello


[<AutoOpen>]
module Helpers =
    // Returns number of primes in range [a,b)
    let primes(a,b) =
        // Naive primality check : http://en.wikipedia.org/wiki/Primality_test#Javascript_implementation
        let isPrime n =
            if n <= 3 then n > 1
            elif n % 2 = 0 || n % 3 = 0 then false
            else 
                let rec aux i =
                    if i * i > n then true
                    elif n % i = 0 || n % (i+2) = 0 then false
                    else aux (i+6)
                aux 5
        let mutable c = 0
        for i = a to b-1 do
            if isPrime i then c <- c + 1
        c

    // Split 0..n to partitions
    let splitRanges partitions n = 
        [| for i in 0..partitions - 1 -> 
               let i, j = n * i / partitions, n * (i + 1) / partitions
               (i, j) |]

// Get number of primes in [0,upTo)
let getPrimes upTo = cloud {
    let! n = Cloud.GetWorkerCount()
    let ranges = splitRanges (n * 2) upTo
    let! results = 
        ranges 
        |> Seq.map (fun range -> cloud { return primes range })
        |> Cloud.Parallel 
    return Array.sum results
}

let ps = client.CreateProcess(getPrimes 100000000)

client.ShowProcess(ps.Id)

// https://primes.utm.edu/howmany.html
ps.AwaitResult()
