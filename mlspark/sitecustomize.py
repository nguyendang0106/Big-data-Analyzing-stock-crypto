"""
Ensure importlib.metadata has packages_distributions on Python 3.8.

Some google client libs import importlib.metadata.packages_distributions, which
is only present in Python >=3.10. On 3.8 we add a shim using the backport
importlib_metadata if the attribute is missing.
"""
import importlib.metadata as _md

if not hasattr(_md, "packages_distributions"):
    try:
        import importlib_metadata
    except Exception:
        import warnings

        warnings.warn(
            "importlib_metadata backport not available; packages_distributions missing"
        )
    else:
        _md.packages_distributions = importlib_metadata.packages_distributions
