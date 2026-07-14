"""Scenario spec -> generated files (rows + faults + logical clock). Deterministic.

Contracts:
  - same scenario name => byte-identical output (seed = (scenario, source, emitter);
    logical clock only — never wall clock) — replay support by construction (R6)
  - every record carries _ingestion_order (row position in its file) — the
    emulator-generated tiebreak component approved in slice 7
  - every fault name used by a packaged scenario has a handler here
    (completeness meta-test); handlers mutate deterministically via the seeded rng
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from retail_lakehouse.config import loader
from retail_lakehouse.config.contracts import ConfigError
from retail_lakehouse.scenarios import parser

# fixed entity pools — consistent across scenarios/days
CUSTOMER_IDS = list(range(1, 51))
PRODUCT_IDS = list(range(1, 21))
STORE_IDS = list(range(1, 6))
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
CATEGORIES = ["Electronics", "Grocery", "Apparel", "Home"]
EVENT_TYPES = ["PLACED", "PAID", "SHIPPED"]
ACTIVITY_TYPES = ["PAGE_VIEW", "SEARCH", "ADD_TO_CART", "REMOVE_FROM_CART", "WISHLIST"]
DEVICES = ["IOS", "ANDROID", "WEB"]


@dataclass(frozen=True)
class GeneratedFile:
    source: str
    emitter: str | None
    file_name: str
    records: list[dict]  # each carries _ingestion_order


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _file_name(source: str, clock: datetime, seq: int, extension: str) -> str:
    return f"{source}_{clock:%Y%m%d}_{clock:%H%M%S}_{seq:03d}.{extension}"


# ---------------------------------------------------------------- row templates
def _base_row(source: str, rng: random.Random, clock: datetime, i: int) -> dict:
    day = clock.strftime("%Y-%m-%d")
    if source == "customers":
        cid = CUSTOMER_IDS[i % len(CUSTOMER_IDS)]
        return {
            "customer_id": cid,
            "first_name": f"F{cid}",
            "last_name": f"L{cid}",
            "email": f"c{cid}@mail.co",
            "phone": f"08{cid:08d}",
            "birth_date": f"19{60 + cid % 40}-0{1 + cid % 9}-15",
            "loyalty_tier": TIERS[cid % 4],
            "home_store_id": STORE_IDS[cid % 5],
            "signup_ts": _iso(clock - timedelta(days=400)),
            "updated_at": _iso(clock),
        }
    if source == "products":
        pid = PRODUCT_IDS[i % len(PRODUCT_IDS)]
        return {
            "product_id": pid,
            "sku": f"SKU-{pid:04d}",
            "product_name": f"Product {pid}",
            "category": CATEGORIES[pid % 4],
            "subcategory": f"Sub{pid % 3}",
            "unit_price": f"{10 + pid}.00",
            "currency": "USD" if pid % 3 else "THB",
            "is_active": "true",
            "effective_date": day,
        }
    if source == "stores":
        sid = STORE_IDS[i % len(STORE_IDS)]
        return {
            "store_id": sid,
            "store_name": f"Store {sid}",
            "region": REGIONS[sid % 4],
            "store_format": "STANDARD",
            "manager_name": f"M{sid}",
            "opened_date": "2020-01-01",
            "closed_date": None,
            "update_date": _iso(clock),
        }
    if source == "customer_updates":
        cid = rng.choice(CUSTOMER_IDS)
        return {
            "op": "U",
            "customer_id": cid,
            "first_name": f"F{cid}",
            "last_name": f"L{cid}",
            "email": f"c{cid}+new@mail.co",
            "phone": f"08{cid:08d}",
            "birth_date": f"19{60 + cid % 40}-0{1 + cid % 9}-15",
            "loyalty_tier": rng.choice(TIERS),
            "home_store_id": STORE_IDS[cid % 5],
            "source_system": rng.choice(["CRM", "MOBILE_APP"]),
            "change_ts": _iso(clock + timedelta(minutes=i)),
            "change_seq": i,
        }
    if source == "order_events":
        oid = 1000 * (clock.day) + i
        return {
            "event_id": f"evt-{clock:%Y%m%d}-{i:05d}",
            "order_id": oid,
            "event_type": EVENT_TYPES[i % 3],
            "customer_id": rng.choice(CUSTOMER_IDS),
            "store_id": rng.choice(STORE_IDS),
            "product_id": rng.choice(PRODUCT_IDS),
            "quantity": 1 + i % 5,
            "unit_price": f"{5 + i % 20}.50",
            "currency": "USD",
            "event_ts": _iso(clock - timedelta(hours=2) + timedelta(minutes=i)),
            "region_source": "north",
        }
    if source == "customer_activity":
        return {
            "activity_id": f"act-{clock:%Y%m%d}-{i:06d}",
            "session_id": f"s-{i // 5:05d}",
            "customer_id": rng.choice(CUSTOMER_IDS) if i % 7 else None,  # anonymous by design
            "activity_type": ACTIVITY_TYPES[i % 5],
            "product_id": rng.choice(PRODUCT_IDS),
            "search_term": None,
            "device": DEVICES[i % 3],
            "activity_ts": _iso(clock - timedelta(hours=1) + timedelta(seconds=30 * i)),
        }
    raise ConfigError(f"no row template for source {source!r}")


# ---------------------------------------------------------------- fault handlers
def _ts_col(source):
    return {
        "customers": "birth_date",
        "stores": "opened_date",
        "customer_updates": "change_ts",
        "order_events": "event_ts",
        "customer_activity": "activity_ts",
    }[source]


FAULT_HANDLERS = {
    "null_key": lambda rs, rng, c, s: rs.__setitem__(0, {**rs[0], "customer_id": None}),
    "row_duplicate": lambda rs, rng, c, s: rs.append(dict(rs[1])),
    "key_duplicate": lambda rs, rng, c, s: rs.append({**rs[2], "email": "changed@mail.co"}),
    "invalid_birth_date": lambda rs, rng, c, s: rs.__setitem__(
        3, {**rs[3], "birth_date": "1899-13-45"}
    ),
    "invalid_loyalty_tier": lambda rs, rng, c, s: rs.__setitem__(
        4, {**rs[4], "loyalty_tier": "DIAMOND"}
    ),
    "broken_store_ref": lambda rs, rng, c, s: rs.__setitem__(5, {**rs[5], "home_store_id": 999}),
    "duplicate_event_id": lambda rs, rng, c, s: rs.append(dict(rs[0])),
    "negative_quantity": lambda rs, rng, c, s: rs.__setitem__(1, {**rs[1], "quantity": -1}),
    "future_event_ts": lambda rs, rng, c, s: rs.__setitem__(
        2, {**rs[2], _ts_col(s): "2030-01-01T00:00:00Z"}
    ),
    "null_customer_id": lambda rs, rng, c, s: rs.__setitem__(3, {**rs[3], "customer_id": None}),
    "null_change_ts": lambda rs, rng, c, s: rs.__setitem__(0, {**rs[0], "change_ts": None}),
    "invalid_op": lambda rs, rng, c, s: rs.__setitem__(1, {**rs[1], "op": "X"}),
    "orphan_update": lambda rs, rng, c, s: rs.__setitem__(2, {**rs[2], "customer_id": 99999}),
    "late_arriving_events": lambda rs, rng, c, s: [
        rs.__setitem__(i, {**rs[i], _ts_col(s): _iso(c - timedelta(days=1, minutes=i))})
        for i in range(min(3, len(rs)))
    ],
    "out_of_order_within_batch": lambda rs, rng, c, s: rs.reverse(),
    "equal_sequence_tiebreak": lambda rs, rng, c, s: rs.__setitem__(
        1,
        {
            **rs[1],
            "customer_id": rs[0]["customer_id"],
            "change_ts": rs[0]["change_ts"],
            "change_seq": rs[0]["change_seq"] + 1,
        },
    ),
    "cancelled_before_placed": lambda rs, rng, c, s: rs.insert(
        0,
        {
            **rs[0],
            "event_id": rs[0]["event_id"] + "-x",
            "event_type": "CANCELLED",
            "event_ts": _iso(c - timedelta(hours=3)),
        },
    ),
    "stale_redelivery": lambda rs, rng, c, s: rs.__setitem__(
        0, {**rs[0], "update_date": _iso(c - timedelta(days=30))}
    ),
    "delete_events": lambda rs, rng, c, s: rs.__setitem__(
        0,
        {
            "op": "D",
            "customer_id": rs[0]["customer_id"],
            "first_name": None,
            "last_name": None,
            "email": None,
            "phone": None,
            "birth_date": None,
            "loyalty_tier": None,
            "home_store_id": None,
            "source_system": rs[0]["source_system"],
            "change_ts": rs[0]["change_ts"],
            "change_seq": rs[0]["change_seq"],
        },
    ),
    "orphan_delete": lambda rs, rng, c, s: rs.append(
        {
            **rs[-1],
            "op": "D",
            "customer_id": 88888,
            "first_name": None,
            "last_name": None,
            "email": None,
            "phone": None,
            "birth_date": None,
            "loyalty_tier": None,
            "home_store_id": None,
            "change_ts": _iso(c),
            "change_seq": 0,
        }
    ),
    "extra_column_south": lambda rs, rng, c, s: [
        rs.__setitem__(i, {**rs[i], "discount_pct": "5"}) for i in range(min(2, len(rs)))
    ],
    "extra_column_south_all_rows": lambda rs, rng, c, s: [
        rs.__setitem__(i, {**rs[i], "discount_pct": "5"}) for i in range(len(rs))
    ],
    "epoch_millis_timestamps": lambda rs, rng, c, s: [
        rs.__setitem__(i, {**rs[i], _ts_col(s): str(int(c.timestamp() * 1000))})
        for i in range(min(3, len(rs)))
    ],
}


def _apply_faults(records: list[dict], faults, rng, clock, source) -> list[dict]:
    for fault in faults or []:
        handler = FAULT_HANDLERS.get(fault)
        if handler is None:
            raise ConfigError(f"no generator handler for fault {fault!r}")
        handler(records, rng, clock, source)
    return records


def _attach_order(records: list[dict]) -> list[dict]:
    return [{**r, "_ingestion_order": i} for i, r in enumerate(records)]


def generate_batches(scenario: dict) -> list[GeneratedFile]:
    """Scenario spec (parser.load_scenario output) -> deterministic file list."""
    if "replay_of" in scenario:  # byte-identical replay of the target scenario
        return generate_batches(parser.load_scenario(scenario["replay_of"]))

    sources = loader.load_sources()
    clock: datetime = scenario["logical_clock"]
    absent = set(scenario.get("expected_absent") or [])
    files: list[GeneratedFile] = []

    for source, spec in scenario["batches"].items():
        config = sources[source]
        extension = "csv" if config["format"] == "csv" else "json"
        rng = random.Random(f"{scenario['scenario']}:{source}")
        records = [_base_row(source, rng, clock, i) for i in range(spec["rows"])]
        records = _apply_faults(records, spec.get("faults"), rng, clock, source)

        emitters = spec.get("emitters") or list(config.get("emitters") or [None])
        emitters = [e for e in emitters if f"{source}.{e}" not in absent]
        if not emitters:
            continue
        chunk = max(1, len(records) // len(emitters))
        for idx, emitter in enumerate(emitters):
            part = (
                records[idx * chunk :]
                if idx == len(emitters) - 1
                else records[idx * chunk : (idx + 1) * chunk]
            )
            if emitter == "south":  # south POS quotes in THB (FX normalization path)
                part = [
                    {
                        **r,
                        "currency": "THB",
                        "unit_price": f"{float(r['unit_price']) * 35:.2f}",
                        "region_source": "south",
                    }
                    for r in part
                ]
            files.append(
                GeneratedFile(
                    source=source,
                    emitter=emitter,
                    file_name=_file_name(source, clock, idx + 1, extension),
                    records=_attach_order(part),
                )
            )
    return files
