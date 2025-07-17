# models/dari_tour_models.py

from pydantic import BaseModel, Field


class DariTourOffer(BaseModel):
    """
    Represents a tour offer from Dari Tour website.
    
    Attributes:
        name: The name/title of the tour offer
        date: The date or date range of the tour
        price: The price of the tour (include currency if available)
        transport_type: Type of transport (e.g., 'Bus', 'Airplane', 'Train')
        link: Full URL to the tour offer page
    """
    
    # Using Field to add descriptions that will be included in the JSON schema
    name: str = Field(..., description="The name/title of the tour offer")
    date: str = Field(..., description="The date or date range of the tour")
    price: str = Field(..., description="The price of the tour (include currency if available)")
    transport_type: str = Field(..., description="Type of transport (e.g., 'Bus', 'Airplane', 'Train')")
    link: str = Field(..., description="Full URL to the tour offer page")
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "Summer Vacation Special",
                "date": "2025-07-15 to 2025-07-25",
                "price": "€1,299",
                "transport_type": "Airplane",
                "link": "https://dari-tour.com/offers/summer-vacation-special"
            }
        }
