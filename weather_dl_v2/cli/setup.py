from setuptools import setup

requirements = ['typer', 'requests']

setup(
    name = "weather-dl-v2",
    packages=["app", "app.subcommands", "app.services"],
    install_requires=requirements,
    version = "0.0.1",
    author = "aniket",
    description = ("This cli tools helps in interacting with weather dl v2 fast API server"),
    entry_points={
        "console_scripts": [
            "weather-dl-v2=app.main:app"
        ]
    }
)
