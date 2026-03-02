.PHONY: test test-all test-network test-fast

# NOTE: All targets create temporary files on disk (GeoTIFFs, BAGs, HDF5,
# GeoPackages, SQLite DBs) under pytest_temporary_data/. 
# pytest auto-cleans old runs before starting

## Offline tests only — safe for CI (no network or S3 downloads)
test:
	pytest

## Full suite — inclusive of network and S3 downloads
test-all:
	pytest -m "" -v -s

## Network tests only — inclusive of S3 downloads
test-network:
	pytest -m network -v -s

## Full suite, skip slow — still inclusive of network and S3 downloads
test-fast:
	pytest -m "not slow" -v -s
