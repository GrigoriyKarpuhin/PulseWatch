package main

import (
	"pulsewatch/internal/db"
	"pulsewatch/internal/run"
)

func main() { run.Service("migrate", db.Migrate) }
