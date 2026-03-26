package main

import (
	"os"

	"github.com/rothgar/k3s-to-talos/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
