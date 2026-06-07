import json
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue


ROOT = Path("/workspace")
INPUT = ROOT / "local-setup/proofs/sample-wallet-events.jsonl"
OUTPUT_DIR = ROOT / "local-setup/beam-wallet-pipeline/output"


class SplitAndEnrichWalletEvent(beam.DoFn):
    ACCEPTED = "accepted"
    DEADLETTER = "deadletter"

    def process(self, raw_line):
        event = json.loads(raw_line)
        amount = float(event.get("amount") or 0)
        event["pipeline"] = "beam-direct-wallet-poc"
        event["risk_tier"] = "HIGH" if amount >= 1000 else "STANDARD"

        if event.get("event_type") == "wallet.payment.authorized" and amount >= 100:
            yield pvalue.TaggedOutput(self.ACCEPTED, json.dumps(event, sort_keys=True))
        else:
            yield pvalue.TaggedOutput(self.DEADLETTER, json.dumps(event, sort_keys=True))


def run():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    options = PipelineOptions(["--runner=DirectRunner"])

    with beam.Pipeline(options=options) as pipeline:
        split = (
            pipeline
            | "Read wallet events" >> beam.io.ReadFromText(str(INPUT))
            | "Filter and enrich" >> beam.ParDo(SplitAndEnrichWalletEvent()).with_outputs(
                SplitAndEnrichWalletEvent.ACCEPTED,
                SplitAndEnrichWalletEvent.DEADLETTER,
            )
        )

        split[SplitAndEnrichWalletEvent.ACCEPTED] | "Write filtered" >> beam.io.WriteToText(
            str(OUTPUT_DIR / "wallet-filtered"),
            file_name_suffix=".jsonl",
            shard_name_template="",
        )
        split[SplitAndEnrichWalletEvent.DEADLETTER] | "Write deadletter" >> beam.io.WriteToText(
            str(OUTPUT_DIR / "wallet-deadletter"),
            file_name_suffix=".jsonl",
            shard_name_template="",
        )


if __name__ == "__main__":
    run()
