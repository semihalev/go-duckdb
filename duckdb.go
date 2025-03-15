package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
)

var driverSingleton sync.Once

func init() {
	driverSingleton.Do(func() {
		sql.Register("duckdb", &Driver{})
	})
}

// Driver implements database/sql/driver.Driver interface.
type Driver struct{}

// Open opens a new connection to the database.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	conn, err := newConnection(dsn)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// OpenConnector returns a new connector for this driver
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return &connector{dsn: dsn, driver: d}, nil
}

// connector is a fixed driver.Connector implementation for Driver
type connector struct {
	dsn    string
	driver *Driver
}

// Connect returns a connection to the database
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	// Check for early context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return c.driver.Open(c.dsn)
}

// Driver returns the connector's driver
func (c *connector) Driver() driver.Driver {
	return c.driver
}
