"""Immutable signed restore-grant claims shared without database dependencies."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat


class RestoreContractError(RuntimeError):
    pass


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def public_key_b64(key: Ed25519PrivateKey) -> str:
    return base64.b64encode(
        key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    ).decode()


def key_id(public_key: str) -> str:
    try:
        raw = base64.b64decode(public_key, validate=True)
        Ed25519PublicKey.from_public_bytes(raw)
    except ValueError as exc:
        raise RestoreContractError("invalid restore verification key") from exc
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class RestoreGrantClaims:
    grant_id: str
    registry_ordinal: int
    issuer_key_id: str
    source_system_id: str
    source_database: str
    source_schema: str
    target_system_id: str
    target_database: str
    target_schema: str
    deployment_id: str
    revision_id: str
    run_id: str
    source_release_generation: int
    source_release_fence: int
    target_release_generation: int
    backup_artifact_sha256: str
    backup_manifest_sha256: str
    schema_digest: str
    cuts_json: str
    cuts_digest: str
    quiesce_manifest_digest: str
    source_finalize_digest: str
    algorithm: str = "Ed25519"
    protocol: str = "ariadne.schema-restore-grant/v1"

    def validate(self) -> None:
        if (
            self.algorithm != "Ed25519"
            or self.protocol != "ariadne.schema-restore-grant/v1"
        ):
            raise RestoreContractError("unsupported restore grant protocol")
        if (
            min(
                self.registry_ordinal,
                self.source_release_generation,
                self.source_release_fence,
                self.target_release_generation,
            )
            <= 0
        ):
            raise RestoreContractError(
                "positive restore authority coordinates required"
            )
        if self.target_release_generation != self.source_release_generation + 1:
            raise RestoreContractError("restore generation must advance exactly once")
        if any(not value for value in asdict(self).values()):
            raise RestoreContractError("complete restore identity required")
        try:
            cuts = json.loads(self.cuts_json)
        except (ValueError, TypeError) as exc:
            raise RestoreContractError("invalid canonical restore cuts") from exc
        if (
            not isinstance(cuts, dict)
            or canonical(cuts) != self.cuts_json
            or digest(cuts) != self.cuts_digest
        ):
            raise RestoreContractError("restore cuts digest mismatch")
        for value in (
            self.issuer_key_id,
            self.backup_artifact_sha256,
            self.backup_manifest_sha256,
            self.schema_digest,
            self.cuts_digest,
            self.quiesce_manifest_digest,
            self.source_finalize_digest,
        ):
            if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
                raise RestoreContractError("invalid restore digest")

    def signed_bytes(self) -> bytes:
        self.validate()
        return canonical(asdict(self)).encode()

    @property
    def cuts(self) -> dict[str, Any]:
        return json.loads(self.cuts_json)


@dataclass(frozen=True)
class BoundGrantWitness:
    claims: RestoreGrantClaims
    signature_b64: str

    @classmethod
    def sign(
        cls, claims: RestoreGrantClaims, key: Ed25519PrivateKey
    ) -> BoundGrantWitness:
        if claims.issuer_key_id != key_id(public_key_b64(key)):
            raise RestoreContractError("restore issuer identity mismatch")
        signature = key.sign(claims.signed_bytes())
        return cls(claims, base64.b64encode(signature).decode())

    def verify(self, pinned_public_key_b64: str) -> None:
        if self.claims.issuer_key_id != key_id(pinned_public_key_b64):
            raise RestoreContractError("restore grant issuer is not pinned")
        try:
            public = Ed25519PublicKey.from_public_bytes(
                base64.b64decode(pinned_public_key_b64, validate=True)
            )
            public.verify(
                base64.b64decode(self.signature_b64, validate=True),
                self.claims.signed_bytes(),
            )
        except (ValueError, InvalidSignature) as exc:
            raise RestoreContractError("restore grant signature rejected") from exc

    def to_dict(self) -> dict[str, Any]:
        return {"claims": asdict(self.claims), "signature_b64": self.signature_b64}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> BoundGrantWitness:
        if set(value) != {"claims", "signature_b64"}:
            raise RestoreContractError("restore grant envelope drift")
        return cls(RestoreGrantClaims(**value["claims"]), value["signature_b64"])

    @property
    def witness_digest(self) -> str:
        return digest(self.to_dict())
