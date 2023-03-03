package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/GreptimeTeam/greptimedb-client-go/pkg/sql"
)

type Monitor struct {
	Host   string
	Cpu    float64
	Memory float64
	Ts     time.Time
}

func main() {
	// Open a GreptimeDB connection with database/sql API.
	// Use `greptimedb` as driverName and a valid DSN to define data source
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
	// Use Next() to iterate over query result lines
	for res.Next() {
		var monitor Monitor
		err := res.Scan(&monitor.Host, &monitor.Cpu, &monitor.Memory, &monitor.Ts)

		if err != nil {
			fmt.Printf("res.Scan err: %v", err)
			continue
		}
		monitors = append(monitors, monitor)
	}

	fmt.Printf("%#v\n", monitors)
}
