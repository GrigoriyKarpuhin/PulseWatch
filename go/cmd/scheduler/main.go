package main

import (
	"pulsewatch/internal/run"
	"pulsewatch/internal/scheduler"
)

func main() { run.Service("scheduler", scheduler.Run) }
