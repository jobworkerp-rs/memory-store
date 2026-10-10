//! Fixed output vocabularies shared by the machine-readable
//! `memories-db-migrate` commands (`local`, `embedding`): every value a
//! caller may branch on is one of a closed set of snake_case words.

macro_rules! vocabulary {
    ($(#[$meta:meta])* $name:ident { $first:ident => $first_text:literal $(, $variant:ident => $text:literal)* $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
        pub enum $name {
            #[default]
            $first,
            $($variant),*
        }

        impl $name {
            pub const ALL: &'static [Self] = &[Self::$first $(, Self::$variant)*];

            pub fn as_str(self) -> &'static str {
                match self {
                    Self::$first => $first_text,
                    $(Self::$variant => $text),*
                }
            }

            pub fn parse(text: &str) -> Option<Self> {
                Self::ALL.iter().copied().find(|value| value.as_str() == text)
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

pub(crate) use vocabulary;

/// Percent-encode everything that could break `key=value` parsing or is not
/// printable ASCII; path separators stay readable.
pub fn encode_value(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' | b':' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}
