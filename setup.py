from setuptools import setup, find_packages

setup(
    name="smallcaps-shared",
    version="1.1.6",
    description="Shared library for SmallCaps Scanner microservices",
    packages=find_packages(),
    package_data={
        "smallcaps_shared": ["data/*.json"],
    },
    include_package_data=True,
    install_requires=[
        "redis>=5.0.0",
        "httpx>=0.25.0",
        "pydantic>=2.0.0",
        "python-dotenv>=1.0.0",
        "asyncpg>=0.29.0",
        "pytz>=2024.1",
        "aiohttp>=3.9.0",
        "prometheus_client>=0.19.0",
    ],
    python_requires=">=3.11",
)