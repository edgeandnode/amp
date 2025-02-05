// Import the necessary modules from the Effect libraries
import { Args, Command } from "@effect/cli"
import { NodeContext, NodeRuntime } from "@effect/platform-node"
import { Console, Effect } from "effect"

// Create a command that logs the provided text argument to the console
const command = Command.make("build", {}, () => {
    return Console.log("build returned")
})

// Configure and initialize the CLI application
const cli = Command.run(command, {
    name: "Nozzle CLI",
    version: "v0.0.1"
})


// Prepare and run the CLI application, providing necessary context and runtime
cli(process.argv).pipe(Effect.provide(NodeContext.layer), NodeRuntime.runMain)