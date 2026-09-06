# Contributing

Contributions are welcome — bug reports, documentation, and code alike.

## Getting started

```bash
git clone https://github.com/CleatSquad/go-grpc-relay.git
cd go-grpc-relay
go build ./...
```

## Before opening a pull request

```bash
go test ./...   # must pass
go vet ./...    # must report no issue
```

## Guidelines

- Match the style of the surrounding code; run `gofmt` before committing.
- Every behaviour change needs a test. Bug fixes need a test that fails before
  the fix.
- Keep the public API small. A new exported symbol is a long-term commitment.
- This library has no runtime dependencies beyond `google.golang.org/grpc`
  and `google.golang.org/protobuf`, and that is deliberate. Pull requests
  adding another one need to make a strong case.
- Update the README when public behaviour changes.

## Backward compatibility

Every exported identifier is part of the public API and follows
[Semantic Versioning](https://semver.org). Breaking it requires a major
release, so prefer additive changes.

## Commit messages

Short imperative subject line, explaining what changes and why.
