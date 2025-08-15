# models/dari_tour_excursions_models.py

from pydantic import BaseModel, Field


class DariTourExcursionOffer(BaseModel):
    """
    Represents an excursion offer from Dari Tour website.
    
    Attributes:
        name: The name/title of the excursion offer
        date: The date or date range of the excursion
        price: The price of the excursion (include currency if available)
        link: Full URL to the excursion offer page
    """
    
    # Using Field to add descriptions that will be included in the JSON schema
    name: str = Field(..., description="The name/title of the excursion offer")
    date: str = Field(..., description="The date or date range of the excursion")
    price: str = Field(..., description="The price of the excursion (include currency if available)")
    link: str = Field(..., description="Full URL to the excursion offer page")
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "Екскурзия до Австралия",
                "date": "09.02.2026, 20.02.2026",
                "price": "16850 лв. / 8615.27 €",
                "link": "https://dari-tour.com/ekskurzia-do-avstralia"
            }
        }
