# models/venue.py

from pydantic import BaseModel


class Offer(BaseModel):
    """
    Represents the data structure of an Offer.
    """

    name: str
    date: str
    price: str
    transport_type: str
    link: str
