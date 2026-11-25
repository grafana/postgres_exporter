// Copyright The Prometheus Authors
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

package collector

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
)

// sqlFilterBuilder constructs parameterized PostgreSQL include/exclude filter
// clauses using pq.Array with sequential positional placeholders ($1, $2, ...).
type sqlFilterBuilder struct {
	conditions []string
	args       []any
}

func (b *sqlFilterBuilder) addIncludeExclude(column string, include, exclude []string) {
	if len(include) > 0 {
		b.args = append(b.args, pq.Array(include))
		b.conditions = append(b.conditions, fmt.Sprintf("%s = ANY($%d)", column, len(b.args)))
	}
	if len(exclude) > 0 {
		b.args = append(b.args, pq.Array(exclude))
		b.conditions = append(b.conditions, fmt.Sprintf("%s != ALL($%d)", column, len(b.args)))
	}
}

func (b *sqlFilterBuilder) whereClause() (string, []any) {
	if len(b.conditions) == 0 {
		return "", nil
	}
	return "WHERE " + strings.Join(b.conditions, " AND "), b.args
}

func (b *sqlFilterBuilder) andClause() (string, []any) {
	if len(b.conditions) == 0 {
		return "", nil
	}
	return "AND " + strings.Join(b.conditions, " AND "), b.args
}
