"""Fresh-process runner used by the Prefect adapter recovery evidence."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json

from prefect_stream_adapter import prefect_stream_flow


async def _run(config_json: str, documents_json: str) -> None:
    config = json.loads(config_json)
    documents = [
        {
            "producer_sequence": item["producer_sequence"],
            "record_id": item["record_id"],
            "lineage_digest": item["lineage_digest"],
            "payload": base64.b64decode(item["payload_b64"], validate=True),
        }
        for item in json.loads(documents_json)
    ]
    state = await prefect_stream_flow(config, documents, return_state=True)
    print(
        "PREFECT_STREAM_RESULT="
        + json.dumps(
            {
                "flow_run_id": str(state.state_details.flow_run_id),
                "state_type": state.type.value,
                "state_name": state.name,
            },
            sort_keys=True,
        ),
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("config_json")
    parser.add_argument("documents_json")
    args = parser.parse_args()
    asyncio.run(_run(args.config_json, args.documents_json))


if __name__ == "__main__":
    main()
