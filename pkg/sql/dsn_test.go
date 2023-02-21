package sql

import (
	req "GreptimeTeam/greptimedb-client-go/pkg/request"
	"reflect"
	"testing"
)

var testDSNs = []struct {
	in  string
	out *req.Config
}{{
	"username:password@protocol(address)/catalogname:dbname",
	&req.Config{UserName: "username", Password: "password", Net: "protocol", Address: "address", CatalogName: "catalogname", DBName: "dbname"},
}, {
	"username:password@protocol(address)/dbname",
	&req.Config{UserName: "username", Password: "password", Net: "protocol", Address: "address", CatalogName: "", DBName: "dbname"},
}, {
	"/",
	&req.Config{Net: "", Address: "127.0.0.1", CatalogName: "", DBName: ""},
},
}

func TestDSNParser(t *testing.T) {
	for i, tst := range testDSNs {
		cfg, err := ParseDSN(tst.in)
		if err != nil {
			t.Error(err.Error())
		}

		if !reflect.DeepEqual(cfg, tst.out) {
			t.Errorf("%d. ParseDSN(%q) mismatch:\ngot  %+v\nwant %+v", i, tst.in, cfg, tst.out)
		}
	}
}
