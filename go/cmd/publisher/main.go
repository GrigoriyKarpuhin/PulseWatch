package main

import (
	"pulsewatch/internal/publisher"
	"pulsewatch/internal/run"
)

func main() { run.Service("publisher", publisher.Run) }
