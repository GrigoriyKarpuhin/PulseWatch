package main

import (
	"pulsewatch/internal/projector"
	"pulsewatch/internal/run"
)

func main() { run.Service("projector", projector.Run) }
