"""
FastAPI backend for the Node.js streaming.
Protocol per line: {"type": "status|token|sources|error|done", ...}
"""
import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os
import json
import logging  
from typing import AsyncGenerator


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator  
from dotenv import load_dotenv

from crawler import crawl_documentation
from rag import create_pipelines
from haystack import Document

load_dotenv()



                

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# App Setup

app = FastAPI(title="DocsChat Crawler", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
   
    allow_origins=os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    url: str = Field(..., description="Documentation site URL")
    question: str = Field(..., description="User question")
    max_pages: int = Field(default=25, ge=1, le=50)
    max_depth: int = Field(default=2, ge=1, le=3)


    @field_validator("url")
    @classmethod
    def url_must_be_http(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v

    @field_validator("question")
    @classmethod
    def question_must_not_be_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("question must not be blank")
        return v.strip()


# Streaming Generator

async def generate_stream(request: QueryRequest) -> AsyncGenerator[str, None]:
    ibm_api_key = os.getenv("IBM_API_KEY")
    if not ibm_api_key:
        yield json.dumps({"type": "error", "message": "IBM_API_KEY not configured."}) + "\n"
        return

    # Phase 1: Crawl
    yield json.dumps({"type": "status", "message": f"Crawling {request.url}..."}) + "\n"
    logger.info("Starting crawl: %s (max_pages=%d, max_depth=%d)",
                request.url, request.max_pages, request.max_depth)

    try:
        crawled = await crawl_documentation(
            request.url,
            max_pages=request.max_pages,
            max_depth=request.max_depth,
        )
    except Exception as e:
        logger.exception("Crawl failed for %s", request.url)
        import traceback
        traceback.print_exc()
        yield json.dumps({"type": "error", "message": f"Crawling failed: {str(e)}"}) + "\n"
        return

    if not crawled:
        yield json.dumps(
            {"type": "error", "message": "No content could be extracted from the provided URL."}
        ) + "\n"
        return

    logger.info("Crawled %d pages from %s", len(crawled), request.url)
    yield json.dumps(
        {"type": "status", "message": f"Crawled {len(crawled)} pages. Building index..."}
    ) + "\n"

    # Phase 2: Build ephemeral pipelines
    queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_running_loop()
    pipeline_done = asyncio.Event()
    result_container: dict = {}

    def streaming_callback(chunk):
        # Haystack calls this from the thread pool during generation.
        # call_soon_threadsafe bridges back to the event loop.
        loop.call_soon_threadsafe(queue.put_nowait, chunk.content)

    try:
        indexing_pipe, query_pipe, _ = create_pipelines(ibm_api_key, streaming_callback)
    except Exception as e:
        logger.exception("Pipeline setup failed")
        yield json.dumps({"type": "error", "message": f"Pipeline setup failed: {str(e)}"}) + "\n"
        return

    haystack_docs = [
        Document(content=doc["content"], meta={"url": doc["url"], "title": doc["title"]})
        for doc in crawled
    ]

    # Phase 3: Run pipeline in background so tokens can stream concurrently
    async def run_pipeline():
        try:
            # Index — blocking call offloaded to thread pool
            await loop.run_in_executor(
                None,
                lambda: indexing_pipe.run({"splitter": {"documents": haystack_docs}}),
            )

            loop.call_soon_threadsafe(
                lambda: asyncio.ensure_future(
                    _send_status(queue, "Index built. Generating answer...")
                )
            )

            # Query — blocking call offloaded to thread pool
            result = await loop.run_in_executor(
                None,
                lambda: query_pipe.run(
                    {
                        "text_embedder": {"text": request.question}, #STICKING POINT (WAS PREV TEXTEMBEDDER)
                        "prompt_builder": {"query": request.question},
                    }
                ),
            )
            result_container["result"] = result
        except Exception as e:
            logger.exception("Pipeline run failed")
            result_container["error"] = str(e)
        finally:
            pipeline_done.set()
            loop.call_soon_threadsafe(queue.put_nowait, None)  # Sentinel — signals end of tokens

    pipeline_task = asyncio.create_task(run_pipeline())

    # Phase 4: Stream tokens as they arrive
    while True:
        try:
            token = await asyncio.wait_for(queue.get(), timeout=2.0)
            if token is None:  # Sentinel from callback
                break
            
            if token:
                yield json.dumps({"type": "token", "content": token}) + "\n"
        except asyncio.TimeoutError:
            if pipeline_done.is_set() and queue.empty():
                break
            continue

  
    try:
        await pipeline_task
    except Exception:
        pass  

    # Phase 5: Error check and source attribution
    if "error" in result_container:
        yield json.dumps({"type": "error", "message": result_container["error"]}) + "\n"
        return

    result = result_container.get("result", {})
    
    retriever_output = result.get("retriever", {})
    retriever_docs = retriever_output.get("documents", [])

    sources = []
    for doc in retriever_docs:
       
        url = doc.meta.get("url")
        if url and url not in sources:
            sources.append(url)

    yield json.dumps({"type": "sources", "urls": sources, "pages_crawled": len(crawled)}) + "\n"
    yield json.dumps({"type": "done"}) + "\n"
    logger.info("Request complete. Sources: %d", len(sources))


async def _send_status(queue: asyncio.Queue, message: str) -> None:
    await queue.put(json.dumps({"type": "status", "message": message}))



# Endpoints

@app.get("/")
def home():
    return {"message": "Welcome to the DocsChat Crawler! \n"
    "Add'query' to the URL!"}

@app.post("/query")
async def query_docs(request: QueryRequest):
    return StreamingResponse(
        generate_stream(request),
        media_type="application/x-ndjson",
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


# import httpx

# # Disables the timeout completely (waits indefinitely)
# response = httpx.get("http://127.0.0.1:8000/docs#/default/query_docs_query_post", timeout=None)
