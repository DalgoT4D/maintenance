"""updates the urls of prefect's airbyte server blocks"""

import argparse
import requests

parser = argparse.ArgumentParser(
    description="Update the urls of prefect's airbyte server blocks"
)
parser.add_argument("url", help="The url of the new airbyte server")
args = parser.parse_args()


def update_airbyte_server_blocks(new_url: str):
    """updates the server host of all airbyte server blocks"""
    blocks = requests.post(
        "http://localhost:4200/api/block_documents/filter", {}, timeout=10
    ).json()

    for b in blocks:
        if b["block_type"]["name"] == "Airbyte Server":
            # print(b["data"])
            # print(b["block_schema_id"])
            # print(b["id"])
            # print(json.dumps(b, indent=2))
            if b["data"]["server_host"] == new_url:
                print(f"Block {b['id']} already has the correct url")
                continue

            r = requests.patch(
                f"http://localhost:4200/api/block_documents/{b['id']}",
                json={
                    "block_schema_id": b["block_schema_id"],
                    "data": {"server_host": new_url},
                    "merge_existing_data": True,
                },
                timeout=10,
            )
            r.raise_for_status()
            print("Updated block", b["id"])


if __name__ == "__main__":
    update_airbyte_server_blocks(args.url)
