from pydantic import BaseModel, Field


class AngelTravelOffer(BaseModel):
    """
    Represents a tour offer from Angel Travel website.
    
    Attributes:
        title: The title of the tour offer
        dates: The date or date range of the tour
        price: The price of the tour (include currency if available)
        transport_type: Type of transport (e.g., 'Bus', 'Airplane', 'Train')
        link: Full URL to the tour offer page
    """
    
    # Using Field to add descriptions that will be included in the JSON schema
    title: str = Field(..., description="The title of the tour offer")
    dates: str = Field(..., description="The date or date range of the tour")
    price: str = Field(..., description="The price of the tour (include currency if available)")
    transport_type: str = Field(..., description="Type of transport (e.g., 'Bus', 'Airplane', 'Train')")
    link: str = Field(..., description="Full URL to the tour offer page")
    main_page_link: str = Field(..., description="URL of the main page containing the iframe for the detailed offer")
    
    class Config:
        """
        Pydantic configuration class for AngelTravelOffer.

        This inner class provides configurations for the Pydantic model,
        such as adding an example JSON schema for documentation purposes.
        """
        json_schema_extra = {
            "example": {
                "title": "Exotic Bali Escape",
                "dates": "2025-08-01 to 2025-08-10",
                "price": "$1,500",
                "transport_type": "Airplane",
                "link": "https://www.angeltravel.bg/exotic-bali-escape"
            }
        }