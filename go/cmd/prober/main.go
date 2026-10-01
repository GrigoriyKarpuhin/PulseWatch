package main

import (
	"pulsewatch/internal/prober"
	"pulsewatch/internal/run"
)

func main() { run.Service("prober", prober.Run) }
