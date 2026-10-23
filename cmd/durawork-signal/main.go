// Command durawork-signal delivers a signal to a workflow's durable inbox.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"durawork/engine"
)

func main() {
	dir := flag.String("dir", "", "data directory")
	id := flag.String("id", "", "workflow id")
	name := flag.String("name", "", "signal name")
	payload := flag.String("payload", "{}", "signal payload JSON")
	flag.Parse()
	if *dir == "" || *id == "" || *name == "" {
		fmt.Fprintln(os.Stderr, "usage: durawork-signal -dir D -id ID -name N -payload JSON")
		os.Exit(2)
	}
	store, err := engine.NewStore(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	sig, err := store.SendSignal(*id, *name, json.RawMessage(*payload))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("SIGNAL SENT seq=%d name=%s\n", sig.Seq, sig.Name)
}
