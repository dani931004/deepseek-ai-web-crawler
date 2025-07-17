# models/dari_tour_models.py

from pydantic import BaseModel


class DariTourOffer(BaseModel):
    """
    Represents the data structure of an DariTourOffer.
    """

    name: str
    date: str
    price: str
    transport_type: str
    link: str
