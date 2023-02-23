package main

import (
	"time"
)

type Monitor struct {
	Host   string    `db:"host,INDEX"`
	Ts     time.Time `db:"ts,TIMESTAMP"`
	Cpu    string    `db:"cpu"`
	Memory string    `db:"memory"`
}
