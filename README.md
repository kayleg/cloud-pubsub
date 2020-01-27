# cloud-pubsub

Provides methods to consume messages from Google PubSub using
[Futures](https://github.com/rust-lang-nursery/futures-rs)
and [Hyper](https://hyper.rs).


## Authentication

Authentication is provided by [rust-goauth](https://github.com/durch/rust-goauth).
The `BaseClient` expects to receive the path to the file containing your Google Cloud
service account JSON key.

### Token Renewal

The JWT token has a short life time and needs to be renewed periodically for long lived processes.

There is a provided helper which will renew the token every `15 mins`:

```
let pubsub = match BaseClient::create(config.google_application_credentials) {
    Err(e) => panic!("Failed to initialize pubsub: {}", e),
        Ok(p) => p,
};

tokio::run(lazy(move || {
    pubsub.spawn_token_renew();
}))
```

### Env Config

[Envy](https://github.com/softprops/envy) is an excellent way to load your config.

```
#[derive(Deserialize)]
struct Config {
    pubsub_subscription: String,
    google_application_credentials: String,
}


fn main() {
    let parsed_env = envy::from_env::<Config>();
    if let Err(e) = parsed_env {
        eprintln!("ENV is not valid: {}", e);
        std::process::exit(1);
    }
    let config = parsed_env.unwrap();

    let pubsub = match BaseClient::create(config.google_application_credentials) {
        Err(e) => panic!("Failed to initialize pubsub: {}", e),
        Ok(p) => p,
    };
}
```

## Subscribing

### Connecting to existing subscription

```
let sub = my_client.subscribe("subscription-name")
```

### Subscribing to a topic

When subscribing to a topic, a random subscription name will be generated. To prevent dangling
subscriptions, you need to explicitly call `subscription.destroy()`.
