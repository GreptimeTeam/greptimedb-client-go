package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "GreptimeTeam/greptimedb-client-go/pkg/sql"
)

type Monitor struct {
	Host   string
	Ts     time.Time
	Cpu    float64
	Memory float64
}

func main() {
	db, err := sql.Open("greptimedb", "(127.0.0.1:4001)/public")
	defer db.Close()

	if err != nil {
		fmt.Printf("sql.Open err: %v", err)
	}

	res, err := db.Query("SELECT * FROM monitor")
	defer res.Close()

	if err != nil {
		fmt.Printf("db.Query err: %v", err)
	}

	var monitors []Monitor
	for res.Next() {
		var monitor Monitor
		err := res.Scan(&monitor.Host, &monitor.Ts, &monitor.Cpu, &monitor.Memory)

		if err != nil {
			fmt.Printf("res.Scan err: %v", err)
			continue
		}
		monitors = append(monitors, monitor)
	}

	fmt.Printf("%#v\n", monitors)
}
