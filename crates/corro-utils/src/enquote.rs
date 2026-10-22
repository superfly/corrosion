/// Enquotes `s` with `quote`.
pub fn enquote(quote: char, s: &str) -> String {
    let count = s.chars().filter(|c| *c == quote || *c == '\\').count();

    // There are only 2 cases of enquote in corrosion and both use a character that is a single byte, no need to calculate
    // with the actual byte length of the quote char
    let mut out = String::with_capacity(s.len() + count + 2);
    out.push(quote);

    if count == 0 {
        out.push_str(s);
    } else {
        for c in s.chars() {
            if c == '\\' || c == quote {
                out.push('\\');
            }

            out.push(c);
        }
    }

    out.push(quote);

    out
}

/// Unquotes `s`.
pub fn unquote(s: &str) -> String {
    if s.chars().count() < 2 {
        return s.to_owned();
    }

    let quote = s.chars().next().unwrap();

    if !matches!(quote, '"' | '\'' | '`') {
        return s.to_owned();
    }

    if s.chars().last().unwrap() != quote {
        return s.to_owned();
    }

    // removes quote characters
    // the sanity checks performed above ensure that the quotes will be ASCII and this will not
    // panic
    let us = &s[1..s.len() - 1];

    unescape(us, quote).unwrap_or_else(|| s.to_owned())
}

fn unescape(s: &str, illegal: char) -> Option<String> {
    let mut chars = s.char_indices();
    let mut unescaped = String::with_capacity(s.len());

    while let Some((_i, c)) = chars.next() {
        if c != '\\' {
            if c == illegal {
                return None;
            }

            unescaped.push(c);
            continue;
        }

        let (i, code) = chars.next()?;

        let advance = |c: &mut std::str::CharIndices<'_>, len: usize| -> Option<&str> {
            let st = s.get(i + 1..i + 1 + len)?;

            for _ in 0..len {
                c.next();
            }

            Some(st)
        };

        let c = match code {
            '\\' | '"' | '\'' | '`' => code,
            'a' => '\x07',
            'b' => '\x08',
            'f' => '\x0c',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'v' => '\x0b',
            // hex
            'x' => {
                let hex = advance(&mut chars, 2)?;
                u8::from_str_radix(hex, 16).ok()? as char
            }
            // unicode
            'u' => {
                let hex = advance(&mut chars, 4)?;
                char::from_u32(u32::from_str_radix(hex, 16).ok()?)?
            }
            'U' => {
                let hex = advance(&mut chars, 8)?;
                char::from_u32(u32::from_str_radix(hex, 16).ok()?)?
            }
            // octal, seems unlikely to be needed?
            '0'..='9' => {
                let octal = s.get(i..i + 3)?;
                chars.next();
                chars.next();
                u8::from_str_radix(octal, 8).ok()? as char
            }
            // Unrecognized escape sequence
            _ => return None,
        };

        unescaped.push(c);
    }

    Some(unescaped)
}

#[cfg(test)]
mod test {
    use super::{enquote as enq, unquote as unq};

    #[test]
    fn enquote() {
        assert_eq!(
            enq('"', r#""Fran & Freddie's Diner	☺\""#),
            r#""\"Fran & Freddie's Diner	☺\\\"""#,
        );
        assert_eq!(enq('"', ""), r#""""#);
        assert_eq!(enq('"', r#"""#), r#""\"""#);

        assert_eq!(
            enq('\'', r#""Fran & Freddie's Diner	☺\""#),
            r#"'"Fran & Freddie\'s Diner	☺\\"'"#,
        );
        assert_eq!(enq('\'', ""), "''");
        assert_eq!(enq('\'', "'"), r#"'\''"#);

        assert_eq!(enq('`', ""), "``");
        assert_eq!(enq('`', "`"), r#"`\``"#);
    }

    #[test]
    fn unquote() {
        assert_eq!(unq(""), "");
        assert_eq!(unq("foobar"), "foobar");
        assert_eq!(unq("'foobar"), "'foobar");
        assert_eq!(unq("'foo'bar'"), "'foo'bar'");
        assert_eq!(unq("'foobar\\'"), "'foobar\\'");
        assert_eq!(unq("'\\q'"), "'\\q'");
        assert_eq!(unq("'\\00'"), "'\\00'");

        assert_eq!(
            unq(r#""\"Fran & Freddie's Diner	☺\\\"""#),
            r#""Fran & Freddie's Diner	☺\""#,
        );
        assert_eq!(unq(r#""""#), "");
        assert_eq!(unq(r#""\"""#), r#"""#);

        assert_eq!(
            unq(r#"'"Fran & Freddie\'s Diner	☺\\"'"#),
            r#""Fran & Freddie's Diner	☺\""#,
        );
        assert_eq!(unq("''"), "");
        assert_eq!(unq(r#"'\''"#), "'");

        assert_eq!(unq("``"), "");
        assert_eq!(unq(r#"`\``"#), "`");

        assert_eq!(unq("'\\n'"), "\n");
        assert_eq!(unq("'\\101b'"), "Ab");
        assert_eq!(unq("'\\x76'"), "\x76");
        assert_eq!(unq("'\\u2714'"), "\u{2714}");
        assert_eq!(unq("'\\U0001f427'"), "\u{1f427}");
    }
}
