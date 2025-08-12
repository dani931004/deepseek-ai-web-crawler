# Import models to make them available when importing from the models package
from .dari_tour_detailed_models import OfferDetails

"""
This __init__.py file makes the 'models' directory a Python package.
It also serves to expose selected models directly when the 'models' package is imported.
"""

__all__ = [
    'OfferDetails',
]