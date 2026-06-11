from typing import Optional

"""Variable types for VariantSpark covariates.

These mirror the Scala VariableType sealed trait hierarchy in
``au.csiro.variantspark.data.VariableType``.

Usage::

    from varspark.variable_type import (
        Continuous,
        Nominal,
        Ordinal,
    )

    vsc.import_covariates("covariates.csv", cov_types={
        "age":    Continuous,
        "sex":    Nominal(2),
        "status": Ordinal(3),
    })
"""

__all__ = [
    "VariableType",
    "Continuous",
    "Nominal",
    "Ordinal",
]


class VariableType:
    """Abstract base class for all variable types.

    Use one of the pre-defined singletons (``ContinuousVariable``,
    ``NominalVariable``, ``OrdinalVariable``) or
    instantiate ``BoundedNominalVariable`` / ``BoundedOrdinalVariable``
    for types that carry a level count.
    """

    def _type_str(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return self._type_str()

    def __repr__(self) -> str:
        return self._type_str()


class _Continuous(VariableType):
    def _type_str(self) -> str:
        return "CONTINUOUS"

    def __call__(self) -> "_Continuous":
        """Allow Continuous() as an alias for the singleton."""
        return self


class _Nominal(VariableType):
    """A nominal (unordered categorical) variable with an optional fixed number of classes.

    :param n_levels: Number of distinct categories.
    """

    def __init__(self, n_levels: Optional[int] = None):
        if n_levels is not None and (not isinstance(n_levels, int) or n_levels < 1):
            raise ValueError(f"n_levels must be a positive integer, got {n_levels!r}")
        self.n_levels = n_levels

    def _type_str(self) -> str:
        return "NOMINAL" if self.n_levels is None else f"NOMINAL({self.n_levels})"

    def __call__(self, n_levels: Optional[int] = None) -> "_Nominal":
        """Allow Nominal(n) as a shorthand for creating a new instance with n levels."""
        return _Nominal(n_levels)


class _Ordinal(VariableType):
    """An ordinal (ordered categorical) variable with an optional fixed number of levels.

    :param n_levels: Number of ordered levels.
    """

    def __init__(self, n_levels: Optional[int] = None):
        if n_levels is not None and (not isinstance(n_levels, int) or n_levels < 1):
            raise ValueError(f"n_levels must be a positive integer, got {n_levels!r}")
        self.n_levels = n_levels

    def _type_str(self) -> str:
        return "ORDINAL" if self.n_levels is None else f"ORDINAL({self.n_levels})"

    def __call__(self, n_levels: Optional[int] = None) -> "_Ordinal":
        """Allow Ordinal(n) as a shorthand for creating a new instance with n levels."""
        return _Ordinal(n_levels)


# Singleton instances – use these directly instead of instantiating the private classes.
Continuous = _Continuous()
Nominal = _Nominal()
Ordinal = _Ordinal()
