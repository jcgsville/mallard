use std::fmt::Write;

use sha2::{Digest, Sha256};

pub fn normalize_body(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}\n")
    }
}

pub fn calculate(previous_hash: Option<&str>, body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(previous_hash.unwrap_or("").as_bytes());
    hasher.update(b"\n");
    hasher.update(normalize_body(body).as_bytes());

    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

pub fn is_valid_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::{calculate, is_valid_hash, normalize_body};

    #[test]
    fn normalizes_body_to_single_trailing_newline() {
        assert_eq!(
            normalize_body("\ncreate table test ();\n\n"),
            "create table test ();\n"
        );
    }

    #[test]
    fn calculates_stable_hashes() {
        assert_eq!(
            calculate(None, "select 1;"),
            "25f048318278a4e1670bffb6bdd9de29acec798dcb8e00bff69ac48ec3dfdb3c"
        );
    }

    #[test]
    fn validates_hash_shape() {
        assert!(is_valid_hash(&"a".repeat(64)));
        assert!(!is_valid_hash("xyz"));
    }
}
