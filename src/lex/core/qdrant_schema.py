"""Shared Qdrant collection schema builder.

All collections use identical vector, sparse vector, and quantisation configuration.
Only the collection name and payload indexes differ per domain.
"""

from qdrant_client.models import (
    Distance,
    Modifier,
    PayloadSchemaType,
    ScalarQuantization,
    ScalarQuantizationConfig,
    ScalarType,
    SparseIndexParams,
    SparseVectorParams,
    VectorParams,
)

from lex.settings import EMBEDDING_DIMENSIONS


def build_collection_schema(
    collection_name: str,
    payload_schema: dict[str, PayloadSchemaType],
) -> dict:
    """Build a standard Qdrant collection schema.

    All Lex collections share:
    - Dense vectors: 1024D OpenAI embeddings with cosine distance
    - Sparse vectors: BM25 with IDF modifier, computed server-side by Qdrant
    - INT8 scalar quantisation (75% memory saving, <1% accuracy loss)
    """
    return {
        "collection_name": collection_name,
        "vectors_config": {
            "dense": VectorParams(
                size=EMBEDDING_DIMENSIONS,
                distance=Distance.COSINE,
            )
        },
        "sparse_vectors_config": {
            "sparse": SparseVectorParams(
                index=SparseIndexParams(on_disk=False),
                modifier=Modifier.IDF,
            )
        },
        "payload_schema": payload_schema,
        "quantization_config": ScalarQuantization(
            scalar=ScalarQuantizationConfig(
                type=ScalarType.INT8,
                quantile=0.99,
                always_ram=True,
            )
        ),
    }
