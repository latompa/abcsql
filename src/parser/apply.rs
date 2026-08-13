use super::ast::*;
use super::datetime::{epoch_days_to_date, date_to_epoch_days, parse_date_str, parse_timestamp_str, format_date, format_timestamp};
pub fn apply_round(val: Value, places: Option<Value>) -> Option<Value> {
    let decimals = match places {
        Some(Value::Int(n)) => n,
        None => 0,
        _ => return None,
    };
    match val {
        Value::Int(n) => Some(Value::Int(n)),
        Value::Float(f) => {
            let factor = 10f64.powi(decimals as i32);
            Some(Value::Float((f * factor).round() / factor))
        }
        _ => None,
    }
}

/// Get a field from a JSON object string; returns the raw value substring
pub fn json_object_get(json: &str, key: &str) -> Option<String> {
    let s = json.trim();
    if !s.starts_with('{') { return None; }
    let inner = &s[1..s.len().saturating_sub(1)];
    let mut pos = 0;
    let bytes = inner.as_bytes();
    while pos < bytes.len() {
        // skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
        if pos >= bytes.len() { break; }
        // parse key string
        if bytes[pos] != b'"' { break; }
        let (k, key_end) = json_parse_string(inner, pos)?;
        pos = key_end;
        // skip whitespace and colon
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
        if pos >= bytes.len() || bytes[pos] != b':' { break; }
        pos += 1;
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
        // parse value extent
        let val_end = json_parse_value_extent(inner, pos)?;
        let val_str = inner[pos..val_end].trim().to_string();
        if k == key {
            return Some(val_str);
        }
        pos = val_end;
        // skip comma
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
        if pos < bytes.len() && bytes[pos] == b',' { pos += 1; }
    }
    None
}

/// Get the nth element (0-indexed) from a JSON array string
pub fn json_array_get(json: &str, idx: usize) -> Option<String> {
    let s = json.trim();
    if !s.starts_with('[') { return None; }
    let inner = &s[1..s.len().saturating_sub(1)];
    let bytes = inner.as_bytes();
    let mut pos = 0;
    let mut count = 0;
    while pos < bytes.len() {
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
        if pos >= bytes.len() { break; }
        let val_end = json_parse_value_extent(inner, pos)?;
        let val_str = inner[pos..val_end].trim().to_string();
        if count == idx {
            return Some(val_str);
        }
        count += 1;
        pos = val_end;
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
        if pos < bytes.len() && bytes[pos] == b',' { pos += 1; }
    }
    None
}

/// Parse a JSON string literal starting at `pos`; returns (unescaped_content, end_pos)
pub fn json_parse_string(s: &str, pos: usize) -> Option<(String, usize)> {
    let bytes = s.as_bytes();
    if bytes.get(pos) != Some(&b'"') { return None; }
    let mut i = pos + 1;
    let mut out = String::new();
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            i += 1;
            out.push(match bytes[i] {
                b'n' => '\n', b't' => '\t', b'r' => '\r',
                b'"' => '"',  b'\\' => '\\', b'/' => '/',
                _ => bytes[i] as char,
            });
        } else if bytes[i] == b'"' {
            return Some((out, i + 1));
        } else {
            out.push(bytes[i] as char);
        }
        i += 1;
    }
    None
}

/// Return the end position (exclusive) of a JSON value starting at `pos`
pub fn json_parse_value_extent(s: &str, pos: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if pos >= bytes.len() { return None; }
    match bytes[pos] {
        b'"' => {
            let mut i = pos + 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' { i += 2; continue; }
                if bytes[i] == b'"' { return Some(i + 1); }
                i += 1;
            }
            None
        }
        b'{' | b'[' => {
            let (open, close) = if bytes[pos] == b'{' { (b'{', b'}') } else { (b'[', b']') };
            let mut depth = 0usize;
            let mut i = pos;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    // skip string
                    i += 1;
                    while i < bytes.len() {
                        if bytes[i] == b'\\' { i += 2; continue; }
                        if bytes[i] == b'"' { i += 1; break; }
                        i += 1;
                    }
                    continue;
                }
                if bytes[i] == open { depth += 1; }
                else if bytes[i] == close { depth -= 1; if depth == 0 { return Some(i + 1); } }
                i += 1;
            }
            None
        }
        _ => {
            // number, bool, null — read until delimiter
            let mut i = pos;
            while i < bytes.len() && !matches!(bytes[i], b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r') {
                i += 1;
            }
            if i > pos { Some(i) } else { None }
        }
    }
}

/// Return true if `container` (JSON object or array) contains all key/values of `contained`
pub fn json_contains(container: &str, contained: &str) -> bool {
    let contained = contained.trim();
    let container = container.trim();
    if contained.starts_with('{') {
        // object containment: every key/value in contained must exist in container
        let bytes = contained.as_bytes();
        let inner = &contained[1..contained.len().saturating_sub(1)];
        let mut pos = 0;
        let ibytes = inner.as_bytes();
        while pos < ibytes.len() {
            while pos < ibytes.len() && ibytes[pos].is_ascii_whitespace() { pos += 1; }
            if pos >= ibytes.len() { break; }
            if ibytes[pos] != b'"' { break; }
            let (key, key_end) = match json_parse_string(inner, pos) { Some(x) => x, None => break };
            pos = key_end;
            while pos < ibytes.len() && ibytes[pos].is_ascii_whitespace() { pos += 1; }
            if pos >= ibytes.len() || ibytes[pos] != b':' { return false; }
            pos += 1;
            while pos < ibytes.len() && ibytes[pos].is_ascii_whitespace() { pos += 1; }
            let val_end = match json_parse_value_extent(inner, pos) { Some(x) => x, None => return false };
            let needle_val = inner[pos..val_end].trim().to_string();
            // check container has this key with equal value
            if json_object_get(container, &key).as_deref() != Some(needle_val.as_str()) {
                return false;
            }
            pos = val_end;
            while pos < ibytes.len() && ibytes[pos].is_ascii_whitespace() { pos += 1; }
            if pos < ibytes.len() && ibytes[pos] == b',' { pos += 1; }
            let _ = bytes; // suppress unused warning
        }
        true
    } else {
        // scalar: exact equality
        container == contained
    }
}

/// Apply a JSON operator to two values; returns None if left is not JSON/String
pub fn apply_json_op(left: &Value, op: &ArithOp, right: &Value) -> Option<Value> {
    let json = match left {
        Value::Json(s) | Value::String(s) => s.clone(),
        _ => return None,
    };
    let key = match right {
        Value::String(k) => k.clone(),
        Value::Int(n) => n.to_string(),
        _ => return None,
    };
    // try object key first, then array index
    let raw = json_object_get(&json, &key)
        .or_else(|| key.parse::<usize>().ok().and_then(|i| json_array_get(&json, i)))?;
    match op {
        ArithOp::JsonGet => Some(Value::Json(raw)),
        ArithOp::JsonGetText => {
            // strip surrounding quotes if present
            if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
                if let Some((s, _)) = json_parse_string(&raw, 0) {
                    return Some(Value::String(s));
                }
            }
            Some(Value::String(raw))
        }
        _ => None,
    }
}

/// Helper: convert a Value into a serde_json::Value
fn value_to_json(val: &Value) -> Option<serde_json::Value> {
    match val {
        Value::Null => Some(serde_json::Value::Null),
        Value::Int(n) => Some(serde_json::Value::Number(serde_json::Number::from(*n))),
        Value::Float(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number),
        Value::Bool(b) => Some(serde_json::Value::Bool(*b)),
        Value::String(s) => Some(serde_json::Value::String(s.clone())),
        Value::Json(s) => Some(serde_json::from_str(s).unwrap_or(serde_json::Value::String(s.clone()))),
        Value::Date(d) => Some(serde_json::Value::String(format_date(*d))),
        Value::Timestamp(ts) => Some(serde_json::Value::String(format_timestamp(*ts))),
        Value::Default => None,
    }
}

/// Determine the JSON type name of a value
pub fn apply_json_typeof(val: &Value) -> Option<Value> {
    let json_str = match val {
        Value::Json(s) => s.clone(),
        Value::String(s) => s.clone(),
        Value::Null => return Some(Value::String("null".to_string())),
        Value::Bool(_) => return Some(Value::String("boolean".to_string())),
        Value::Int(_) | Value::Float(_) => return Some(Value::String("number".to_string())),
        Value::Date(_) => return Some(Value::String("date".to_string())),
        Value::Timestamp(_) => return Some(Value::String("timestamp".to_string())),
        Value::Default => return None,
    };
    let v: serde_json::Value = serde_json::from_str(&json_str).ok()?;
    Some(Value::String(match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }.to_string()))
}

/// Return the length of a JSON array
pub fn apply_json_array_length(val: &Value) -> Option<Value> {
    let s = match val {
        Value::Json(s) | Value::String(s) => s,
        _ => return None,
    };
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    match v {
        serde_json::Value::Array(arr) => Some(Value::Int(arr.len() as i64)),
        _ => None,
    }
}

/// Build a JSON object from key-value pairs
pub fn apply_json_build_object(pairs: &[(Value, Value)]) -> Option<Value> {
    let mut map = serde_json::Map::new();
    for (k, v) in pairs {
        let key = match k {
            Value::String(s) => s.clone(),
            _ => return None,
        };
        map.insert(key, value_to_json(v)?);
    }
    Some(Value::Json(serde_json::Value::Object(map).to_string()))
}

/// Build a JSON array from values
pub fn apply_json_build_array(vals: &[Value]) -> Option<Value> {
    let arr: Vec<serde_json::Value> = vals.iter().filter_map(|v| value_to_json(v)).collect();
    Some(Value::Json(serde_json::Value::Array(arr).to_string()))
}

pub fn apply_concat(parts: Vec<Option<Value>>) -> Option<Value> {
    let mut result = String::new();
    for part in parts {
        match part {
            Some(Value::String(s))    => result.push_str(&s),
            Some(Value::Json(s))      => result.push_str(&s),
            Some(Value::Int(n))       => result.push_str(&n.to_string()),
            Some(Value::Float(f))     => result.push_str(&f.to_string()),
            Some(Value::Bool(b))      => result.push_str(if b { "true" } else { "false" }),
            Some(Value::Date(d))      => result.push_str(&format_date(d)),
            Some(Value::Timestamp(ts))=> result.push_str(&format_timestamp(ts)),
            Some(Value::Null) | Some(Value::Default) | None => return None,
        }
    }
    Some(Value::String(result))
}

/// Evaluate SUBSTR(str, start [, len]) — 1-indexed, like SQL
pub fn apply_substr(s: Value, start: Value, len: Option<Value>) -> Option<Value> {
    let s = match s { Value::String(s) => s, _ => return None };
    let start = match start { Value::Int(n) => n, _ => return None };
    // SQL SUBSTR is 1-indexed; clamp to valid range
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len() as i64;
    let idx = if start >= 1 { (start - 1).min(n) as usize } else { 0 };
    let slice = &chars[idx..];
    let result: String = match len {
        Some(Value::Int(l)) => slice.iter().take(l.max(0) as usize).collect(),
        None => slice.iter().collect(),
        _ => return None,
    };
    Some(Value::String(result))
}

/// Evaluate REPLACE(str, from, to)
pub fn apply_replace(s: Value, from: Value, to: Value) -> Option<Value> {
    match (s, from, to) {
        (Value::String(s), Value::String(f), Value::String(t)) => Some(Value::String(s.replace(&*f, &*t))),
        _ => None,
    }
}

/// Evaluate IS [NOT] TRUE / FALSE / UNKNOWN boolean tests. NULL and non-boolean
/// operands count as unknown. Returns None for operators that aren't boolean tests.
pub fn eval_boolean_test(op: &Operator, val: Option<&Value>) -> Option<bool> {
    let truth = match val {
        Some(Value::Bool(b)) => Some(*b),
        _ => None,
    };
    match op {
        Operator::IsTrue => Some(truth == Some(true)),
        Operator::IsNotTrue => Some(truth != Some(true)),
        Operator::IsFalse => Some(truth == Some(false)),
        Operator::IsNotFalse => Some(truth != Some(false)),
        Operator::IsUnknown => Some(truth.is_none()),
        Operator::IsNotUnknown => Some(truth.is_some()),
        _ => None,
    }
}

/// Evaluate TRANSLATE(str, from_chars, to_chars): each char found in from_chars is
/// replaced by the char at the same position in to_chars, or dropped if to_chars is shorter.
pub fn apply_translate(s: Value, from: Value, to: Value) -> Option<Value> {
    match (s, from, to) {
        (Value::String(s), Value::String(f), Value::String(t)) => {
            let to_chars: Vec<char> = t.chars().collect();
            let result: String = s.chars()
                .filter_map(|c| match f.chars().position(|fc| fc == c) {
                    Some(i) => to_chars.get(i).copied(),
                    None => Some(c),
                })
                .collect();
            Some(Value::String(result))
        }
        _ => None,
    }
}

/// Evaluate LPAD(str, len, pad)
pub fn apply_lpad(s: Value, len: Value, pad: Value) -> Option<Value> {
    let s = match s { Value::String(s) => s, _ => return None };
    let len = match len { Value::Int(n) => n.max(0) as usize, _ => return None };
    let pad = match pad { Value::String(p) => p, _ => return None };
    if pad.is_empty() { return Some(Value::String(s)); }
    let current = s.chars().count();
    if current >= len {
        return Some(Value::String(s.chars().take(len).collect()));
    }
    let needed = len - current;
    let pad_chars: Vec<char> = pad.chars().collect();
    let prefix: String = pad_chars.iter().cycle().take(needed).collect();
    Some(Value::String(format!("{}{}", prefix, s)))
}

/// Evaluate RPAD(str, len, pad)
pub fn apply_rpad(s: Value, len: Value, pad: Value) -> Option<Value> {
    let s = match s { Value::String(s) => s, _ => return None };
    let len = match len { Value::Int(n) => n.max(0) as usize, _ => return None };
    let pad = match pad { Value::String(p) => p, _ => return None };
    if pad.is_empty() { return Some(Value::String(s)); }
    let current = s.chars().count();
    if current >= len {
        return Some(Value::String(s.chars().take(len).collect()));
    }
    let needed = len - current;
    let pad_chars: Vec<char> = pad.chars().collect();
    let suffix: String = pad_chars.iter().cycle().take(needed).collect();
    Some(Value::String(format!("{}{}", s, suffix)))
}

/// Evaluate CAST(value AS type)
pub fn apply_cast(val: Value, type_name: &str) -> Option<Value> {
    match type_name {
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => match val {
            Value::Int(n)       => Some(Value::Int(n)),
            Value::Float(f)     => Some(Value::Int(f as i64)),
            Value::Bool(b)      => Some(Value::Int(b as i64)),
            Value::String(s) | Value::Json(s) => s.trim().parse::<i64>().ok().map(Value::Int),
            Value::Date(d)      => Some(Value::Int(d as i64)),
            Value::Timestamp(ts)=> Some(Value::Int(ts)),
            Value::Null         => Some(Value::Null),
            Value::Default      => None,
        },
        "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => match val {
            Value::Float(f)  => Some(Value::Float(f)),
            Value::Int(n)    => Some(Value::Float(n as f64)),
            Value::String(s) | Value::Json(s) => s.trim().parse::<f64>().ok().map(Value::Float),
            Value::Null      => Some(Value::Null),
            _ => None,
        },
        "TEXT" | "VARCHAR" | "STRING" | "CHAR" => match val {
            Value::String(s) | Value::Json(s) => Some(Value::String(s)),
            Value::Int(n)       => Some(Value::String(n.to_string())),
            Value::Float(f)     => Some(Value::String(f.to_string())),
            Value::Bool(b)      => Some(Value::String(b.to_string())),
            Value::Date(d)      => Some(Value::String(format_date(d))),
            Value::Timestamp(ts)=> Some(Value::String(format_timestamp(ts))),
            Value::Null         => Some(Value::Null),
            Value::Default      => None,
        },
        "JSON" | "JSONB" => match val {
            Value::Json(s) | Value::String(s) => Some(Value::Json(s)),
            Value::Null => Some(Value::Null),
            _ => None,
        },
        "BOOLEAN" | "BOOL" => match val {
            Value::Bool(b)   => Some(Value::Bool(b)),
            Value::Int(n)    => Some(Value::Bool(n != 0)),
            Value::String(s) | Value::Json(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Some(Value::Bool(true)),
                "false" | "0" | "no" => Some(Value::Bool(false)),
                _ => None,
            },
            Value::Null => Some(Value::Null),
            _ => None,
        },
        "DATE" => match val {
            Value::Date(d)      => Some(Value::Date(d)),
            Value::Timestamp(ts)=> Some(Value::Date((ts / 86400) as i32)),
            Value::String(s) | Value::Json(s) => parse_date_str(&s).map(Value::Date),
            Value::Int(n)       => Some(Value::Date(n as i32)),
            Value::Null         => Some(Value::Null),
            _ => None,
        },
        "TIMESTAMP" => match val {
            Value::Timestamp(ts)=> Some(Value::Timestamp(ts)),
            Value::Date(d)      => Some(Value::Timestamp(d as i64 * 86400)),
            Value::String(s) | Value::Json(s) => parse_timestamp_str(&s).map(Value::Timestamp),
            Value::Int(n)       => Some(Value::Timestamp(n)),
            Value::Null         => Some(Value::Null),
            _ => None,
        },
        _ => None, // unknown type
    }
}

/// Evaluate GREATEST(args) — return max non-NULL arg, or NULL if all NULL
pub fn apply_greatest(args: Vec<Option<Value>>) -> Option<Value> {
    let non_null: Vec<Value> = args.into_iter().flatten()
        .filter(|v| !matches!(v, Value::Null))
        .collect();
    non_null.into_iter().max_by(cmp_values_for_sort)
}

/// Evaluate LEAST(args) — return min non-NULL arg, or NULL if all NULL
pub fn apply_least(args: Vec<Option<Value>>) -> Option<Value> {
    let non_null: Vec<Value> = args.into_iter().flatten()
        .filter(|v| !matches!(v, Value::Null))
        .collect();
    non_null.into_iter().min_by(cmp_values_for_sort)
}

fn cmp_values_for_sort(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Int(x), Value::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(x), Value::Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Evaluate POWER(base, exp)
pub fn apply_power(base: Value, exp: Value) -> Option<Value> {
    let b = match base { Value::Int(n) => n as f64, Value::Float(f) => f, _ => return None };
    let e = match exp  { Value::Int(n) => n as f64, Value::Float(f) => f, _ => return None };
    Some(Value::Float(b.powf(e)))
}

/// Evaluate POSITION(needle IN haystack) — 1-based, 0 if not found
pub fn apply_position(needle: Value, haystack: Value) -> Option<Value> {
    let n = match needle   { Value::String(s) => s, _ => return None };
    let h = match haystack { Value::String(s) => s, _ => return None };
    let pos = h.find(n.as_str()).map(|i| i + 1).unwrap_or(0);
    Some(Value::Int(pos as i64))
}

/// Evaluate REPEAT(str, n)
pub fn apply_repeat(s: Value, n: Value) -> Option<Value> {
    let s = match s { Value::String(s) => s, _ => return None };
    let n = match n { Value::Int(n) if n >= 0 => n as usize, _ => return None };
    Some(Value::String(s.repeat(n)))
}

/// Apply a single-arg scalar function to a resolved Value
pub fn apply_scalar_func(func: &ScalarFunc, val: Value) -> Option<Value> {
    match (func, val) {
        (ScalarFunc::Upper,  Value::String(s)) => Some(Value::String(s.to_uppercase())),
        (ScalarFunc::Lower,  Value::String(s)) => Some(Value::String(s.to_lowercase())),
        (ScalarFunc::Length, Value::String(s)) => Some(Value::Int(s.len() as i64)),
        (ScalarFunc::CharLength, Value::String(s)) => Some(Value::Int(s.chars().count() as i64)),
        (ScalarFunc::OctetLength, Value::String(s)) => Some(Value::Int(s.len() as i64)),
        (ScalarFunc::Trim,   Value::String(s)) => Some(Value::String(s.trim().to_string())),
        (ScalarFunc::TrimChars(mode, chars), Value::String(s)) => {
            let trimmed = match chars {
                Some(set) => {
                    let pred = |c: char| set.contains(c);
                    match mode {
                        TrimMode::Leading => s.trim_start_matches(pred).to_string(),
                        TrimMode::Trailing => s.trim_end_matches(pred).to_string(),
                        TrimMode::Both => s.trim_matches(pred).to_string(),
                    }
                }
                None => match mode {
                    TrimMode::Leading => s.trim_start().to_string(),
                    TrimMode::Trailing => s.trim_end().to_string(),
                    TrimMode::Both => s.trim().to_string(),
                },
            };
            Some(Value::String(trimmed))
        }
        (ScalarFunc::LTrim,  Value::String(s)) => Some(Value::String(s.trim_start().to_string())),
        (ScalarFunc::RTrim,  Value::String(s)) => Some(Value::String(s.trim_end().to_string())),
        (ScalarFunc::Abs, Value::Int(n))    => Some(Value::Int(n.abs())),
        (ScalarFunc::Abs, Value::Float(f))  => Some(Value::Float(f.abs())),
        (ScalarFunc::Ceil,  Value::Float(f)) => Some(Value::Float(f.ceil())),
        (ScalarFunc::Ceil,  Value::Int(n))   => Some(Value::Int(n)),
        (ScalarFunc::Floor, Value::Float(f)) => Some(Value::Float(f.floor())),
        (ScalarFunc::Floor, Value::Int(n))   => Some(Value::Int(n)),
        (ScalarFunc::Sqrt,  Value::Float(f)) => Some(Value::Float(f.sqrt())),
        (ScalarFunc::Sqrt,  Value::Int(n))   => Some(Value::Float((n as f64).sqrt())),
        (ScalarFunc::Sign,  Value::Int(n))   => Some(Value::Int(n.signum())),
        (ScalarFunc::Sign,  Value::Float(f)) => Some(Value::Float(f.signum())),
        (ScalarFunc::Trunc, Value::Float(f)) => Some(Value::Float(f.trunc())),
        (ScalarFunc::Trunc, Value::Int(n))   => Some(Value::Int(n)),
        (ScalarFunc::Reverse, Value::String(s)) => Some(Value::String(s.chars().rev().collect())),
        // Date extraction from Date values
        (ScalarFunc::Year,  Value::Date(d))  => Some(Value::Int(epoch_days_to_date(d).0 as i64)),
        (ScalarFunc::Month, Value::Date(d))  => Some(Value::Int(epoch_days_to_date(d).1 as i64)),
        (ScalarFunc::Day,   Value::Date(d))  => Some(Value::Int(epoch_days_to_date(d).2 as i64)),
        // Date extraction from Timestamp values
        (ScalarFunc::Year,  Value::Timestamp(ts)) => Some(Value::Int(epoch_days_to_date((ts / 86400) as i32).0 as i64)),
        (ScalarFunc::Month, Value::Timestamp(ts)) => Some(Value::Int(epoch_days_to_date((ts / 86400) as i32).1 as i64)),
        (ScalarFunc::Day,   Value::Timestamp(ts)) => Some(Value::Int(epoch_days_to_date((ts / 86400) as i32).2 as i64)),
        (ScalarFunc::Hour,   Value::Timestamp(ts)) => Some(Value::Int((ts % 86400) / 3600)),
        (ScalarFunc::Minute, Value::Timestamp(ts)) => Some(Value::Int((ts % 86400 % 3600) / 60)),
        (ScalarFunc::Second, Value::Timestamp(ts)) => Some(Value::Int(ts % 60)),
        // DayOfWeek: Unix epoch (1970-01-01) was Thursday=4; 0=Sun,1=Mon,...,6=Sat
        (ScalarFunc::DayOfWeek, Value::Date(d)) => Some(Value::Int(((d % 7 + 4) % 7) as i64)),
        (ScalarFunc::DayOfWeek, Value::Timestamp(ts)) => {
            let d = (ts / 86400) as i32;
            Some(Value::Int(((d % 7 + 4) % 7) as i64))
        }
        (ScalarFunc::DayOfYear, Value::Date(d)) => {
            let (y, _, _) = epoch_days_to_date(d);
            let jan1 = date_to_epoch_days(y, 1, 1);
            Some(Value::Int((d - jan1 + 1) as i64))
        }
        (ScalarFunc::DayOfYear, Value::Timestamp(ts)) => {
            let d = (ts / 86400) as i32;
            let (y, _, _) = epoch_days_to_date(d);
            let jan1 = date_to_epoch_days(y, 1, 1);
            Some(Value::Int((d - jan1 + 1) as i64))
        }
        // Allow string input by coercing to date
        (ScalarFunc::Year, Value::String(s))  => parse_date_str(&s).map(|d| Value::Int(epoch_days_to_date(d).0 as i64)),
        (ScalarFunc::Month, Value::String(s)) => parse_date_str(&s).map(|d| Value::Int(epoch_days_to_date(d).1 as i64)),
        (ScalarFunc::Day, Value::String(s))   => parse_date_str(&s).map(|d| Value::Int(epoch_days_to_date(d).2 as i64)),
        // Hour/Minute/Second from Date are always 0
        (ScalarFunc::Hour, Value::Date(_))   => Some(Value::Int(0)),
        (ScalarFunc::Minute, Value::Date(_)) => Some(Value::Int(0)),
        (ScalarFunc::Second, Value::Date(_)) => Some(Value::Int(0)),
        _ => None,
    }
}
