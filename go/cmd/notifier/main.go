package main

import (
	"pulsewatch/internal/notifier"
	"pulsewatch/internal/run"
)

func main() { run.Service("notifier", notifier.Run) }
