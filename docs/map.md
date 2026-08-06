# Map

The `map` transform rewrites each record's `value` from a **closed numeric expression** over
the record's own `value` and its numeric attributes. It is stateless and per-record.

```yaml
transforms:
  - type: map
    id: error_rate
    input: joined
    value: "errors / total"   # the expression, assigned to `value`
    on_missing: skip          # skip | error (default skip)
```

Typical uses: derive a rate (`errors / total`), rescale a unit (`value / 1000` for ns to us,
`sent * 8` for bytes to bits), or - after a `join` - combine two series (`a - b`).

## Expression grammar

Operands:

- **`value`** - the record's value (`f64`).
- **a bare name** - a numeric attribute, looked up by key. Dotted OTel-style names are one
  operand: `http.server.duration`.
- **a number literal** - `1000`, `1.5`.

Operators, highest precedence first:

| Operator | Meaning | Associativity |
|---|---|---|
| `^` | power | right (`2 ^ 3 ^ 2` = `2 ^ (3 ^ 2)` = 512) |
| unary `-` | negate | - (binds looser than `^`, so `-2 ^ 2` = `-4`) |
| `* / %` | multiply, divide, modulo | left |
| `+ -` | add, subtract | left |
| `( )` | grouping | - |

The language is deliberately **closed**: no strings, booleans, conditionals, functions, or I/O.
That keeps it cheap to evaluate and statically checkable - `headrace validate` rejects a
malformed expression before the pipeline runs. Functions (`min`, `sqrt`, ...) and more operators
are planned later; arbitrary per-record logic is what the `wasm` transform (roadmap) is for.

## Missing fields and undefined results

`on_missing` decides what happens when the expression can't produce a usable number:

- **`skip`** (default) - drop the record and count it on `headrace.records.dropped`.
- **`error`** - fail the pipeline.

This currently covers three cases together: a referenced field is **absent**, a referenced field
is **present but non-numeric**, and the result is **non-finite** (e.g. divide by zero). Splitting
these into separate policies is under discussion.

## Examples

```yaml
# requests-per-second is already a rate; convert a p99 latency from ns to ms
- type: map
  id: p99_ms
  input: p99_ns
  value: "value / 1000000"

# error ratio from two attributes carried on the record
- type: map
  id: ratio
  input: counts
  value: "errors / total"
  on_missing: skip
```
