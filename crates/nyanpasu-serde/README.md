# nyanpasu-serde

Scoped derives for decoding string-keyed maps directly into typed fields and an
optional unknown-field map. Known fields never pass through Serde's `flatten`
`Content` buffer. Field definitions and Serde attributes remain the source of
truth; no separate key list or handwritten map visitor is needed.

```rust
use std::collections::BTreeMap;
use nyanpasu_serde::{DeserializeFields, DeserializeMap};

#[derive(DeserializeFields)]
struct Metadata {
    #[serde(rename = "sourcePort", default)]
    source_port: Option<u16>,
}

#[derive(DeserializeMap)]
struct Connection {
    id: String,
    #[serde(flatten)]
    metadata: Metadata,
    #[serde(flatten)]
    #[nyanpasu_serde(extra)]
    extra: BTreeMap<String, serde_json::Value>,
}

let connection: Connection = serde_json::from_str(
    r#"{"id":"one","sourcePort":443,"extension":{"nested":[null,true]}}"#,
).unwrap();
assert_eq!(connection.metadata.source_port, Some(443));
assert!(connection.extra.contains_key("extension"));
```

`DeserializeMap` implements `serde::Deserialize`. `DeserializeFields` supplies a
flattenable group; it can coexist with Serde's regular `Deserialize` derive when
the group also needs independent decoding. A group cannot contain an extra sink,
and a `DeserializeMap` model cannot be used as a flattened group. These rules
prevent an inner extra sink from silently losing unknown fields.

Supported inputs are named structs with ordinary fields, at most one flattened
group, and at most one explicit extra sink. Groups can themselves flatten one
other group. Extra sinks support `BTreeMap<String, V>` and, with the `indexmap`
feature, `IndexMap<String, V, S>`, where `V: Deserialize`. Unknown duplicate keys
replace the value; IndexMap retains their first position. The outer model takes
precedence if a name overlaps its flattened group.

The derives use Serde's attribute parser (`serde_derive_internals`, pinned because
its API is unstable). Supported decoding attributes are container `rename` and
`rename_all`, and field `rename`, `alias`, `default` (including a function path),
`with`, `deserialize_with`, `skip_deserializing`, and `flatten`. Serialization-only
field attributes `serialize_with`, `skip_serializing`, and `skip_serializing_if`
are accepted for coexistence with `Serialize`. Flattened fields cannot have
decoding modifiers. Other Serde attributes and unnamed structs/enums produce
compile errors. This is intentionally a limited map decoder, not a replacement
for all of Serde derive. Sequence-form struct decoding is unsupported.
Recursive models (including recursion hidden behind a type alias) are outside
this derive's scope; generic-bound inference for recursive models is not provided.
Direct recursive field types and overlapping names/aliases produce compile errors.

Missing ordinary `Option` fields, including type aliases, become `None`. Defaults
apply only to missing fields, not invalid values. Custom decoders without an
explicit default require the field to be present. Duplicate known fields are
rejected, including aliases and a first value of `null`. Borrowed field types
can borrow from the input when the underlying deserializer permits it; `borrow`
attributes for containers such as `Cow` are not supported yet.

All generated methods carry `#[inline(always)]`. There is no intermediate map or
known-key `String` allocation, including transient escaped keys and reader input.
Owned keys from owned input are moved into extras. Field values and retained
unknown data still allocate as their types require; removing the flatten buffer
does not make an owned model zero-copy. The hidden runtime module is exclusively
for matching generated code and is not a stable handwritten adapter API.

Run the behavior tests with `cargo test -p nyanpasu-serde --all-features` and the
attribute/inline checks with `cargo test -p nyanpasu-serde-derive`. Connection-level
regression and allocation probes live in `clash-api/tests` and `clash-api/examples`.
