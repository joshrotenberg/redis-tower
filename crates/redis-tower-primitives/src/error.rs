use std::time::Duration;

// Redis's Lua 5.1 numbers are IEEE-754 doubles. Reserve half the exact-integer
// range for the server's epoch timestamp before adding a GCRA window.
const MAX_SAFE_LUA_DURATION_MICROS: u128 = 1_u128 << 52;

/// Invalid primitive configuration detected before Redis is contacted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigurationError {
    /// A key parameter was empty or contained only whitespace.
    #[error("{parameter} must not be empty")]
    EmptyKey {
        /// Name of the invalid parameter.
        parameter: &'static str,
    },

    /// The lock and fencing counter were configured with the same key.
    #[error("lock_key and fencing_key must be different Redis keys")]
    SameLockAndFencingKey,

    /// A duration that must be positive was zero.
    #[error("{parameter} must be greater than zero")]
    ZeroDuration {
        /// Name of the invalid parameter.
        parameter: &'static str,
    },

    /// A duration could not be represented by the Redis script.
    #[error("{parameter} is too large to represent safely in Redis")]
    DurationTooLarge {
        /// Name of the invalid parameter.
        parameter: &'static str,
    },

    /// A rate-limit quota was zero.
    #[error("quota must be greater than zero")]
    ZeroQuota,

    /// The quota was finer-grained than the script's microsecond clock.
    #[error("quota {quota} exceeds the window's {window_micros} microsecond cells")]
    QuotaExceedsWindowResolution {
        /// Requested quota.
        quota: u32,
        /// Required window rounded up to microseconds.
        window_micros: u64,
    },

    /// A renewal interval was not shorter than the lease TTL.
    #[error("renewal interval {interval:?} must be shorter than lock TTL {ttl:?}")]
    RenewalIntervalNotShorter {
        /// Requested renewal interval.
        interval: Duration,
        /// Configured lock TTL.
        ttl: Duration,
    },
}

pub(crate) fn require_key(
    key: impl Into<String>,
    parameter: &'static str,
) -> Result<String, ConfigurationError> {
    let key = key.into();
    if key.trim().is_empty() {
        return Err(ConfigurationError::EmptyKey { parameter });
    }
    Ok(key)
}

pub(crate) fn duration_millis(
    duration: Duration,
    parameter: &'static str,
) -> Result<u64, ConfigurationError> {
    if duration.is_zero() {
        return Err(ConfigurationError::ZeroDuration { parameter });
    }

    let millis = duration.as_millis();
    let rounded = millis + u128::from(!duration.subsec_nanos().is_multiple_of(1_000_000));
    if rounded > i64::MAX as u128 {
        return Err(ConfigurationError::DurationTooLarge { parameter });
    }
    Ok(rounded as u64)
}

pub(crate) fn duration_micros(
    duration: Duration,
    parameter: &'static str,
) -> Result<u64, ConfigurationError> {
    if duration.is_zero() {
        return Err(ConfigurationError::ZeroDuration { parameter });
    }

    let micros = duration.as_micros();
    let rounded = micros + u128::from(!duration.subsec_nanos().is_multiple_of(1_000));
    if rounded > MAX_SAFE_LUA_DURATION_MICROS {
        return Err(ConfigurationError::DurationTooLarge { parameter });
    }
    Ok(rounded as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_durations_round_up() {
        assert_eq!(duration_millis(Duration::from_nanos(1), "ttl").unwrap(), 1);
        assert_eq!(
            duration_micros(Duration::from_nanos(1_001), "window").unwrap(),
            2
        );
    }

    #[test]
    fn redis_durations_reject_zero() {
        assert_eq!(
            duration_millis(Duration::ZERO, "ttl"),
            Err(ConfigurationError::ZeroDuration { parameter: "ttl" })
        );
    }

    #[test]
    fn lua_duration_reserves_exact_integer_space_for_server_time() {
        let seconds = ((1_u64 << 52) / 1_000_000) + 1;
        assert_eq!(
            duration_micros(Duration::from_secs(seconds), "window"),
            Err(ConfigurationError::DurationTooLarge {
                parameter: "window"
            })
        );
    }
}
