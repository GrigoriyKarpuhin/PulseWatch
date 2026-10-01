package main

import (
	"pulsewatch/internal/api"
	"pulsewatch/internal/run"
)

func main() { run.Service("api", api.Serve) }
