import typer, json, os, sys

app = typer.Typer(pretty_exceptions_show_locals=False)

# This callback applies to *all* commands
@app.callback()
def common_params(ctx: typer.Context,
                  db_url: str = typer.Option("http://localhost:9200", envvar="DK_ELASTIC_URL", help="URL to our elastic host")):
    assert ctx.obj is None

    # For now these are env vars and not params yet
    ctx.obj = {
        "db_url": db_url,
    }
