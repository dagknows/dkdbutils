import typer
from dbutils import root

app = root.app

# app.add_typer(packages.app, name="packages")

if __name__ == "__main__":
    app()
