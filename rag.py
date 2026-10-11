"""
Haystack 2.0 + Chroma RAG pipeline factory.
Creates an ephemeral in-memory collection per request for isolation.
"""
import uuid
import logging 
from typing import Callable, Optional

from haystack import Pipeline
from haystack.components.preprocessors import DocumentSplitter
from haystack.components.writers import DocumentWriter
from haystack.components.builders import PromptBuilder
from haystack_integrations.components.generators.openrouter import OpenRouterChatGenerator
from haystack.utils import Secret
from haystack_integrations.components.embedders.mistral import (MistralTextEmbedder, MistralDocumentEmbedder)
from haystack_integrations.document_stores.chroma import ChromaDocumentStore
from haystack_integrations.components.retrievers.chroma import ChromaEmbeddingRetriever





logger = logging.getLogger(__name__)
import os
MISTRAL_EMBED_API_KEY = os.getenv("MISTRAL_EMBED_API_KEY")


SPLIT_BY = "word"
SPLIT_LENGTH = 300   
SPLIT_OVERLAP = 50
TOP_K = 5


PROMPT_TEMPLATE = """You are a precise technical documentation assistant.
Your answers must be based solely on the context passages provided below.
Where possible, cite the source URL inline (e.g. "According to [url] ...").

Context:
{% for doc in documents %}
[Source: {{ doc.meta['url'] }}]
{{ doc.content }}
{% endfor %}

Question: {{ query }}

If the context does not contain enough information to answer the question,
respond with: "I could not find the answer in the provided documentation."

Answer:"""


def create_pipelines(
    ibm_api_key: str,
    streaming_callback: Optional[Callable] = None,
) -> tuple[Pipeline, Pipeline, ChromaDocumentStore]:

    """
    Build an indexing pipeline and a query pipeline backed by an ephemeral
    Chroma collection.

    Returns: (indexing_pipeline, query_pipeline, document_store)
    """
    # Unique collection per request guarantees isolation between concurrent users.
    collection_name = f"rag_{uuid.uuid4().hex[:12]}"
    logger.info("Creating Chroma collection: %s", collection_name)

    document_store = ChromaDocumentStore(collection_name=collection_name)

    # --- Indexing: chunk → embed → write to Chroma ---
    indexing_pipeline = Pipeline()
    indexing_pipeline.add_component(
        "splitter",
        DocumentSplitter(
            split_by=SPLIT_BY,
            split_length=SPLIT_LENGTH,
            split_overlap=SPLIT_OVERLAP,
        ),
    )
    indexing_pipeline.add_component(
        "embedder",
        MistralDocumentEmbedder(
            api_key = Secret.from_token(MISTRAL_EMBED_API_KEY),
            model="mistral-embed",
        ),
    )
    indexing_pipeline.add_component("writer", DocumentWriter(document_store))
    indexing_pipeline.connect("splitter.documents", "embedder.documents")
    indexing_pipeline.connect("embedder.documents", "writer.documents")

    # --- Query: retrieve → prompt → generate ---
    query_pipeline = Pipeline()

    query_pipeline.add_component(
        "text_embedder",
        MistralTextEmbedder(
            api_key = Secret.from_token(MISTRAL_EMBED_API_KEY),
            model="mistral-embed",
        ),
    )

    query_pipeline.add_component(
        "retriever",
        ChromaEmbeddingRetriever(document_store=document_store, top_k=TOP_K),
    )
    query_pipeline.add_component(
        "prompt_builder",
        PromptBuilder(template=PROMPT_TEMPLATE),
    )
    query_pipeline.add_component(
        "generator",
        OpenRouterChatGenerator(
            model="ibm-granite/granite-4.1-8b",
            api_key=Secret.from_env_var("IBM_API_KEY"),
            streaming_callback=streaming_callback,
        ),
    )

    query_pipeline.connect("text_embedder.embedding", "retriever.query_embedding")
    query_pipeline.connect("retriever.documents", "prompt_builder.documents")
    query_pipeline.connect("prompt_builder.prompt", "generator.messages")

    return indexing_pipeline, query_pipeline, document_store
