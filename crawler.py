"""
Crawler module using crawl4ai with BFSDeepCrawlStrategy.
Uses documentation-oriented URL patterns.
"""
import logging  
from urllib.parse import urlparse
from typing import List, Dict

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.filters import FilterChain, URLPatternFilter, DomainFilter
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy

logger = logging.getLogger(__name__)


MIN_CONTENT_LENGTH = 200


DOC_URL_PATTERNS = [
    "*.html",
    "*/doc/*",
    "*/docs/*",       
    "*/library/*",   
    "*/tutorial/*",
    "*/api/*",
    "*/reference/*",
    "*/generated/*",
    "*/guide/*",      
]


async def crawl_documentation(
    start_url: str,
    max_pages: int = 30,
    max_depth: int = 2,
) -> List[Dict[str, str]]:
    """
    Crawl a documentation site using bounded BFS.

    Args:
        start_url:  Root URL of the documentation to crawl.
        max_pages:  Maximum number of pages to visit (default 30).
        max_depth:  BFS depth limit relative to start_url (default 2).

    Returns:
        List of dicts: [{"url": "...", "title": "...", "content": "..."}]
    """
    parsed = urlparse(start_url)

    if not parsed.netloc:
        raise ValueError(f"Could not parse a valid domain from URL: {start_url!r}")

    allowed_domain = parsed.netloc
    logger.info("Crawling domain=%s max_pages=%d max_depth=%d", allowed_domain, max_pages, max_depth)

    filters = [
        DomainFilter(allowed_domains=[allowed_domain]),
        URLPatternFilter(patterns=DOC_URL_PATTERNS),
    ]

    config = CrawlerRunConfig(
        deep_crawl_strategy=BFSDeepCrawlStrategy(
            max_depth=max_depth,
            max_pages=max_pages,
            include_external=False,
            filter_chain=FilterChain(filters),
        ),
       
        scraping_strategy=LXMLWebScrapingStrategy(),
        verbose=False,
    )

    documents: List[Dict[str, str]] = []

    browser_config = BrowserConfig(
        browser_type="chromium",
        use_managed_browser=False,
        chrome_channel="chrome",  # use system Chrome instead of Playwright's bundled Chromium
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun(url=start_url, config=config)

        # Defensive: deep crawl should return a list, but normalise just in case.
        if not isinstance(results, list):
            results = [results]

        for result in results:
        
            content = (result.markdown.raw_markdown
                       if hasattr(result.markdown, "raw_markdown")
                       else result.markdown)

            if (
                result.success
                and content
                and len(content.strip()) >= MIN_CONTENT_LENGTH
            ):
                meta = getattr(result, "metadata", {}) or {}
                documents.append(
                    {
                        "url": result.url,
                        "title": meta.get("title", "Untitled"),
                        "content": content,  
                    }
                )

    logger.info("Crawl complete: %d usable pages found", len(documents))
    return documents
