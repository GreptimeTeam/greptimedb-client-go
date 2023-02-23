package main

import (
	"time"
)

type Monitor struct {
	Host   string    `db: "host, TAG"`
	Ts     time.Time `db: "ts, TIMESTAMP"`
	Cpu    float64    `db: "cpu, FIELD"`
	Memory float64    `db: "memory, FIELD"`
}
