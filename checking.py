# import asyncio
# from crawler import crawl_documentation

# asyncio.run(crawl_documentation("https://docs.python.org/3/", max_pages=5, max_depth=1))
# # print(result.markdown[:300])  # Print first 300 chars





"""
Standalone diagnostic script.

Run this directly with `python debug_crawl.py` from inside your backend/
directory (same folder as crawler.py). It bypasses FastAPI entirely so any
exception raised by crawl_documentation() prints its full type and traceback
instead of being swallowed into the empty str(e) you saw in the API response.
"""
import asyncio
import traceback

from crawler import crawl_documentation


async def main():
    try:
        docs = await crawl_documentation(
            "https://docs.python.org/3/",
            max_pages=5,
            max_depth=1,
        )
        print(f"Success: {len(docs)} pages crawled.")
        for d in docs[:3]:
            print(f" - {d['url']} ({len(d['content'])} chars)")
    except Exception as e:
        print(f"Exception type: {type(e).__name__}")
        print(f"Exception repr: {e!r}")
        print("Full traceback:")
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
