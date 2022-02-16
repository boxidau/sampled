libsampled - py3
---

Allows developers to emit samples for ingestion into SampleD

### Sample Collection

Sample collection is non-blocking. Samples are sent for ingestion in a background thread.

Samples are batched for ingest once either `SAMPLED_BUFFER_FLUSH_SIZE` samples have been collected or `SAMPLED_BUFFER_FLUSH_INTERVAL_MS` milliseconds have passed since the last batch was sent.

On shutdown of your python program samples may be left un-sent, samples are always best-effort delivery.

A sample is a dictionary with `str` keys and `Union[str, int, float, List[str]]` values.

### Usage Example

```
from libsampled import sampled, canonical

def main() {
    // do your logic

    sampled.add_sample(dataset="my_awesome_dataset", sample={
        "hostname": canonical.hostname(),
        "logic": __name__,
        "foo": 123,
        "bar": 0.98,
        "baz": ["a", "b", "c"]
    })
}

if __name__ == "__main__":
    main()

```

### Configuration

The following environment variables are available:

| Environment Variable | Description | Default |
|----------------------|-------------|---------|
| SAMPLED_BUFFER_FLUSH_SIZE | How many samples to keep before sending for ingest | 100 |
| SAMPLED_BUFFER_FLUSH_INTERVAL_MS | Interval in milliseconds between batches being sent | 1000 |
| SAMPLED_DAEMON_HOST | host/ipv4/ipv6 address to send samples to | localhost |
| SAMPLED_DAEMON_PORT | TCP port number to send samples to | 7675 |