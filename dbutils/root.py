import typer, json, os, sys

app = typer.Typer(pretty_exceptions_show_locals=False)

# This callback applies to *all* commands
@app.callback()
def common_params(ctx: typer.Context,
                  db_url: str = typer.Option("http://localhost:9200", envvar="DK_ES_URL", help="URL to our elastic host"),
                  current_index: str = typer.Option(..., envvar="DK_ES_CURRENT_INDEX", help="Name of the current index to operate on"),
                  index_version: str = typer.Option("", envvar="DK_ES_INDEX_VERSION", help="The current index version of the current index to operate on, eg _alias, __0 etc.  These will be suffixed to the current_index")):
    assert ctx.obj is None
    if not current_index:
        raise Exception("current_index is required.")

    # For now these are env vars and not params yet
    ctx.obj = {
        "db_url": db_url,
        "current_index": current_index,
        "index_version": index_version,
    }
