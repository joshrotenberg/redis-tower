use docker_wrapper::{RedisTemplate, testing::ContainerGuard};
use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct RedisFixture {
    _guard: ContainerGuard<RedisTemplate>,
    address: String,
}

impl RedisFixture {
    pub async fn start(image_reference: &str) -> Result<Self, Box<dyn Error>> {
        let (image, tag) = split_image_reference(image_reference);
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let name = format!("redis-tower-chaos-{}-{timestamp}", std::process::id());
        let template = RedisTemplate::new(name).custom_image(image, tag).port(0);

        let guard = ContainerGuard::new(template)
            .capture_logs(true)
            .wait_for_ready(true)
            .stop_timeout(Duration::from_secs(1))
            .start()
            .await?;
        let host_port = guard.host_port(6379).await?;

        Ok(Self {
            _guard: guard,
            address: format!("127.0.0.1:{host_port}"),
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }
}

fn split_image_reference(reference: &str) -> (&str, &str) {
    let last_slash = reference.rfind('/');
    let tag_separator = reference
        .rfind(':')
        .filter(|separator| last_slash.is_none_or(|slash| separator > &slash));

    match tag_separator {
        Some(separator) => (&reference[..separator], &reference[separator + 1..]),
        None => (reference, "latest"),
    }
}

#[cfg(test)]
mod tests {
    use super::split_image_reference;

    #[test]
    fn splits_standard_image_reference() {
        assert_eq!(
            split_image_reference("redis:8.8-alpine"),
            ("redis", "8.8-alpine")
        );
    }

    #[test]
    fn splits_namespaced_image_reference() {
        assert_eq!(
            split_image_reference("valkey/valkey:8.1-alpine"),
            ("valkey/valkey", "8.1-alpine")
        );
    }

    #[test]
    fn preserves_registry_port() {
        assert_eq!(
            split_image_reference("registry.example:5000/redis:8.8"),
            ("registry.example:5000/redis", "8.8")
        );
    }

    #[test]
    fn defaults_an_untagged_image_to_latest() {
        assert_eq!(split_image_reference("redis"), ("redis", "latest"));
    }
}
