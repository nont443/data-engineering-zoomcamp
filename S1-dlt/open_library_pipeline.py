"""`dlt` pipeline to ingest data from the Open Library Search API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source(query: str = "harry potter book", limit: int = 50):
    """Open Library REST API resources (Search API)."""
    config: RESTAPIConfig = {
        "client": {
            # Open Library Search API docs:
            # https://openlibrary.org/dev/docs/api/search
            "base_url": "https://openlibrary.org/",
        },
        "resource_defaults": {
            "primary_key": "key",
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "search",
                "endpoint": {
                    "path": "search.json",
                    "params": {
                        "q": query,
                        "limit": limit,
                        "fields": "key,title,author_name,first_publish_year,edition_count,cover_i,ia",
                    },
                    "data_selector": "docs",
                    # For this exercise, just fetch the first page of results.
                    "paginator": {"type": "single_page"},
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    dataset_name="open_library",
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
