package main

import (
	"time"
)

type Monitor struct {
	Host   string    `db: "host, TAG"`
	Ts     time.Time `db: "ts, TIMESTAMP"`
	Cpu    string    `db: "cpu, FIELD"`
	Memory string    `db: "memory, FIELD"`
}
