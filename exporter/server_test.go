// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !integration

package exporter

import (
	. "gopkg.in/check.v1"
)

type ServersSuite struct{}

var _ = Suite(&ServersSuite{})

const serversSuiteDSN = "postgresql://user:pass@127.0.0.1:5432/db?sslmode=disable"

// Close must disconnect every server it knows about.
func (s *ServersSuite) TestCloseClosesServers(c *C) {
	servers := NewServers()

	// sql.Open doesn't connect, so no database is needed.
	server, err := NewServer(serversSuiteDSN)
	c.Assert(err, IsNil)
	servers.servers[serversSuiteDSN] = server

	servers.Close()

	c.Check(server.db.Ping(), ErrorMatches, "sql: database is closed")
}

// A scrape that's still running when the collection is closed asks for
// servers it hasn't opened yet. It must not get new connections that nothing
// is left to close.
func (s *ServersSuite) TestGetServerAfterCloseDoesNotOpenConnections(c *C) {
	servers := NewServers()
	servers.Close()

	server, err := servers.GetServer(serversSuiteDSN)

	c.Check(err, Equals, ErrServersClosed)
	c.Check(server, IsNil)
	c.Check(servers.servers, HasLen, 0)
}

// A server that was known when the collection was closed is not handed out
// again afterwards.
func (s *ServersSuite) TestGetKnownServerAfterClose(c *C) {
	servers := NewServers()

	server, err := NewServer(serversSuiteDSN)
	c.Assert(err, IsNil)
	servers.servers[serversSuiteDSN] = server

	servers.Close()

	got, err := servers.GetServer(serversSuiteDSN)

	c.Check(err, Equals, ErrServersClosed)
	c.Check(got, IsNil)
}
