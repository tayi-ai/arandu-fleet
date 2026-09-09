---
name: cluster-package
description: Install, wire and use the Arandu Cluster package (Go, Arandu) in an application. Use when the request is to "install Arandu Cluster", "add cluster to the app", "go get github.com/tayi-ai/arandu-fleet", "wire it into bootstrap/app.go", "register the module", "use the cluster routes", "everything under /cluster returns 403", "403 forbidden from cluster", "the table does not exist", "no such table", "change where it is mounted", "let admins read it", or when a project's go.mod already requires github.com/tayi-ai/arandu-fleet. Covers the three lines of wiring and where each one goes, the Config fields and which one is required, the routes and their names, why the policy refuses everything until somebody opens it, and the migration step that is not optional.
license: MIT
---

# Using Arandu Cluster

An Arandu package with one entity, its own table, its own routes and its own
policy. It is registered by hand — there is no service provider, no container
and no discovery, so if a line is not written in the application, it does not
happen.

## Install

```bash
go get github.com/tayi-ai/arandu-fleet
```

## The three lines of wiring, and where each one goes

All three are in `bootstrap/app.go`. Nothing else in the application changes,
and `aru make:module` does not edit this file for you.

The import, with the other module imports:

```go
import (
	cluster "github.com/tayi-ai/arandu-fleet"
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

`New` returns an error rather than starting half-wired. It refuses a
configuration that cannot work, a nil database handle and a nil session store,
so a wiring mistake costs one restart instead of arriving as a nil dereference
on the first request that needed it.

## Then run the migrations

```bash
aru migrate
```

Not optional, and not automatic. The package owns a table, which is why its
`arandu.mod.toml` declares `migrations = true` — that declaration is how an
installer finds out before deploying rather than from the first request.

`aru migrate` is a step in the pipeline and never a call at the start of the
process: with N replicas, N migrations race each other.

**Symptom to recognise:** `no such table`, or `relation does not exist`, from a
route that authorized correctly. The migration has not run.

## Configuration

| field | required | meaning |
| --- | --- | --- |
| `Tenant` | yes | the customer a visitor with no session is read as. From the application's configuration, never from the request |
| `Prefix` | no | where the routes are mounted. Defaults to `/cluster` |
| `PageSize` | no | how many records one page answers with. Defaults to 25, refused above 200 |

`Tenant` is the one place a tenant does not come from a `Grant`, and it is
because a visitor with no session has no `Grant` yet. Everywhere else the tenant
comes from `data.Tenant(g)`. Passing a value the request named — a path segment,
a header, a subdomain read off the URL — is what `TestNoTenantIsReadOutOfTheRequest`
fails on. In an application the same mistake is `aru doctor`'s
`tenant-from-request`; in a package nothing but the package's own suite looks.

`PageSize` above 200 is an error from `New`, not a silent 200. A number somebody
wrote and did not get is worse than a number somebody wrote and was told about.

## Routes

| method | path | name |
| --- | --- | --- |
| `GET` | `/cluster` | `cluster.index` |
| `GET` | `/cluster/{id}` | `cluster.show` |
| `POST` | `/cluster` | `cluster.store` |

Build URLs from the names, never by writing the path a second time. `aru
route:list` shows them grouped by module. Under a custom `Prefix` the paths move
and the names do not.

`GET /cluster` answers `{"data": {"items": [...]}}`, and adds
`"next_cursor"` beside `data` when a further page exists. Pass it back as
`?cursor=`. A page shorter than `PageSize` is the last one and carries no
cursor.

`TenantID` is not in any response. That is deliberate: it names a customer's
identifier and the response is a declared list of fields rather than the entity.

## Every route refuses everybody, until you open the policy

This is the state the package ships in, and it is not a bug to work around.
`ClusterPolicy` denies every action and has no branch that allows one:

```
403 forbidden
```

is what a correctly installed, correctly migrated, correctly wired package
answers to an administrator on day one.

Opening an action means writing the rule that opens it, in the package's
`policy.go`, inside the block that says so:

```go
	// arandu:begin custom
	if a == ClusterView && (s.ID == record.ID || s.HasRole("admin")) {
		return nil
	}
	// arandu:end custom
```

What is not written there stays closed, including every action added later.
There are five actions — `ClusterView`, `ClusterList`, `ClusterCreate`,
`ClusterUpdate`, `ClusterDelete` — and opening one opens exactly one.

**Do not open it from the application.** There is no hook, no override and no
config flag for the policy, and adding one would be a second place where
authorization is decided. If the rules this application needs are not the rules
the package ships, the change belongs in the package.

## What the answers mean

| status | what happened |
| --- | --- |
| `403` | the policy refused, or no rule allows the action yet |
| `404` | no row with that id in this tenant |
| `422` | the input was rejected; the body names the fields |

A refusal carries no detail beyond the status and a word. Telling a client why a
policy said no tells it what exists and what does not, one request at a time.
The reason is in the log.

Anything else is a 500 and the framework's error page in development. The
package returns unexpected errors rather than swallowing them, so a route that
answers 200 with an empty body is not something it does.

## Reporting a problem

Issues and pull requests go to the repository the module path names,
`github.com/tayi-ai/arandu-fleet`, which belongs to `tayi-ai`. A vulnerability goes to the
private advisory form named in that repository's `SECURITY.md`, and never into
an issue.

MIT licensed. Copyright Paulo R. Lima.
