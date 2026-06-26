mod ast;
mod datetime;
mod stmt;
mod cond;
mod apply;

pub use ast::*;
pub use datetime::*;
pub use stmt::*;
pub use cond::*;
pub use apply::*;

#[cfg(test)]
mod tests;

/// Strip SQL comments from input, preserving string literals.
pub fn strip_sql_comments(input: &str) -> String { strip_comments(input) }

/// Strip SQL comments from input, preserving string literals.
/// Handles -- line comments and /* block comments */.
fn strip_comments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let n = chars.len();
    let mut i = 0;
    while i < n {
        // Inside single-quoted string — pass through verbatim including ''
        if chars[i] == '\'' {
            out.push('\'');
            i += 1;
            loop {
                if i >= n { break; }
                if chars[i] == '\'' {
                    out.push('\'');
                    i += 1;
                    // '' is an escaped quote inside the string
                    if i < n && chars[i] == '\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break; // end of string literal
                    }
                } else {
                    out.push(chars[i]);
                    i += 1;
                }
            }
        // -- line comment: skip to end of line
        } else if i + 1 < n && chars[i] == '-' && chars[i + 1] == '-' {
            while i < n && chars[i] != '\n' { i += 1; }
        // /* block comment */: skip to */
        } else if i + 1 < n && chars[i] == '/' && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < n && !(chars[i] == '*' && chars[i + 1] == '/') { i += 1; }
            i += 2; // consume */
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}
