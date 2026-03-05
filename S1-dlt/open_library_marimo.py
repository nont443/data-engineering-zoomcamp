# open_library_marimo.py

import marimo

__generated_with = "0.20.2"
app = marimo.App()


@app.cell
def _():
    import dlt
    import ibis
    import plotly.express as px

    pipeline = dlt.pipeline(
        pipeline_name="open_library_pipeline",
        destination="duckdb",
        dataset_name="open_library",
    )
    dataset = pipeline.dataset()
    con = dataset.ibis()

    search = con.table("search")

    # dlt often normalizes list fields into child tables like `search__author_name`.
    # Prefer the normalized table if present, otherwise fall back to a column on `search`.
    try:
        authors = con.table("search__author_name")
        author_col = (
            authors.value.name("author")
            if "value" in authors.columns
            else authors.author_name.name("author")
        )
        parent_id_col = (
            authors._dlt_parent_id if "_dlt_parent_id" in authors.columns else authors._dlt_id
        )
        books_per_author = (
            authors.mutate(author=author_col)
            .group_by("author")
            .aggregate(book_count=parent_id_col.nunique())
            .order_by(ibis.desc("book_count"))
            .limit(20)
        )
    except Exception:
        if "author_name" not in search.columns:
            raise RuntimeError(
                "Could not find author names. Expected `search__author_name` table "
                "or `author_name` column in `search`."
            )

        author_col = (
            search.author_name[0].name("author")
            if search.schema()["author_name"].is_array()
            else search.author_name.name("author")
        )
        books_per_author = (
            search.mutate(author=author_col)
            .group_by("author")
            .aggregate(book_count=search.count())
            .order_by(ibis.desc("book_count"))
            .limit(20)
        )

    df = books_per_author.to_pandas()
    fig = px.bar(df, x="author", y="book_count", title="Number of books per author (top 20)")
    return


@app.cell
def _():
    import marimo as mo

    mo.md("# Open Library – Books per Author")
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
