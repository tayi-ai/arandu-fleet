# Arandu Cluster

An Arandu package. It registers its own routes, owns its own table, and decides
for itself who may reach either.

## Install

```bash
go get github.com/tayi-ai/arandu-cluster
```

## Wire it

An Arandu application registers a module explicitly. There is no service
provider, no container and no discovery, so these are the lines to paste into
`bootstrap/app.go` and there are no others.

The import, with the other module imports:

```go
import (
	cluster "github.com/tayi-ai/arandu-cluster"
)
```

The construction, in `Build`, after the session store exists and before
`k.Register`:

```go
	clusterModule, err := cluster.New(cluster.Config{
		Tenant: cfg.Auth.Tenant,
	}, db, sessions)
	if err != nil {
		return App{}, err
	}
```

And the registration, inside the `k.Register(...)` call already there:

```go
		clusterModule,
```

Then, once, before the application serves:

```bash
aru migrate
```

This package owns a table, which is why the migration step is not optional and
why `arandu.mod.toml` says `migrations = true`.

## Publish the views

This package carries the markup of its own pages and hands it over instead of
rendering it from the inside, because a page you cannot edit is a page that says
the wrong thing in your product.

Look at what would be written, then write it:

```bash
aru vendor:publish --tag=view
aru vendor:publish --tag=view --apply
```

Nothing is written without `--apply`. The preview lists every file as `create`,
`update`, `unchanged` or `conflict`, and running the command a second time
writes nothing. A file changed outside its `arandu:begin custom` markers is
reported as a conflict and left alone; `--force` publishes over one, and even
then what is inside the markers is carried forward.

The files land under `resources/views/vendor/cluster/`, and from that point
they are yours. Nothing of this package is compiled beside them, so no view name
is registered twice and no rule has to decide which of two files won — the
consequence being that a view of this package that changes later does not reach
a project that already published it.

Two steps are left to you, and they are left to you because a command that
edited `bootstrap/app.go` behind your back is a command whose output nobody can
explain. Compile what was written:

```bash
aru view:build
```

and import the directory it wrote into, with the other imports:

```go
	_ "your/module/path/storage/framework/views/vendor/cluster"
```

Without that import the views are not in the binary, and the module refuses to
boot rather than answering the first request that reaches one of them with a
500. The refusal names the view, the command and the import.

## Configuration

| field | required | meaning |
| --- | --- | --- |
| `Tenant` | yes | the customer a visitor with no session is read as. From the application's configuration, never from the request. |
| `Prefix` | no | where the routes are mounted. Defaults to `/cluster`. |
| `PageSize` | no | how many records one page answers with. Defaults to 25, refused above 200. |

`New` returns an error rather than starting half-wired, so a setting that
cannot work fails where it is written instead of on the first request that
needed it.

## Routes

| method | path | name |
| --- | --- | --- |
| `GET` | `/cluster` | `cluster.index` |
| `GET` | `/cluster/{id}` | `cluster.show` |
| `POST` | `/cluster` | `cluster.store` |

Every one of them is refused until the policy is opened. That is the state the
package ships in, and it is deliberate.

## Open the policy

`policy.go` denies every action and has no branch that allows one. Open what
this package needs, one action at a time, inside the custom block:

```go
	// arandu:begin custom
	if a == ClusterView && (s.ID == record.ID || s.HasRole("admin")) {
		return nil
	}
	// arandu:end custom
```

What is not written there stays closed, including every action added later.

## Model-first data path

`Cluster` embeds `model.Model[Cluster]`, and `Clusters(db)` is the one
configured entry point for its table. `ClusterService` owns `*data.DB` and
follows `validate -> security.Authorize -> Grant -> Model terminal`; handlers
never hold the database or construct a Model.

Create writes `TenantID` from `data.Tenant(g)`. Find authorizes before reading
and again against the row it found. List authorizes before building its scoped,
allowlisted query. The Model keeps its default `tenant_id` scope on every
terminal.

Terminals return `*Cluster` and `[]*Cluster`. Keep those pointers intact:
copying an embedded Model leaves its `Entity` pointer aimed at the original
allocation. `Resource` and `Collection` are explicit response snapshots and do
not expose tenant or Model internals.

There is no CRUD Repository. Add one only for a complex query, read model,
report, export or raw SQL contract that the common Model path cannot express.

## Layout

```
module.go      registration, routes, handlers and migrations
config.go      what the application passes in
model.go       the entity, and what it may answer with
policy.go      who may do what
service.go     the rules and authorized Model access
views.go       the files the application takes ownership of
```

## What is already correct, and has to stay that way

**The policy denies everything.** There is no permit-all branch to delete
later. The Service calls `security.Authorize` before its first `Clusters(db)`
reach, and every Model terminal requires the Grant that call produced.

**Authorization precedes the Model.** The package audit checks that order in
every exported Service method. A Model terminal enforces tenant scope; the
preceding Policy call decides whether the action itself is allowed.

**The tenant comes from `data.Tenant(g)`.** Never from the path, the body, the
query string or a header. The value on the Grant came from the session; a value
that arrived with the request is a value the caller chose.

**`arandu.mod.toml` declares what the package does** — network, filesystem,
exec, migrations — and the suite compares the declaration against what the code
*calls*, not against what it imports: `net/http` is imported by everything with
a route and says nothing. A package that says it makes no outbound calls and
then opens one fails its own tests, and that is the only place the comparison
happens. `aru doctor` audits the application it is run inside and never loads a
dependency, so nothing audits an installed package except the package itself.

## Tests

```bash
go test -race ./...
```

The denial suite constructs the Service with a nil database, so even building
`Clusters(nil)` would panic. The structural twin reads the allowed path and
rejects any Service method that reaches the Model before `Authorize`.

## Licence

MIT. See [LICENSE.md](LICENSE.md). Copyright Paulo R. Lima.
