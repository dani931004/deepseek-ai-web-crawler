from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class Hotel(BaseModel):
    name: str = Field(..., description="The name of the hotel")
    price: str = Field(..., description="The price of the hotel stay")
    country: str = Field(..., description="The country and number of nights")

class OfferDetails(BaseModel):
    # This is the main model that encapsulates all the details of an offer.
    offer_name: str = Field(..., description="The name of the offer, usually found in the H1 tag.")
    hotels: List[Hotel] = Field(..., description="List of hotels available for the offer, each with its name, price, and country/nights.")
    program: str = Field(..., description="The detailed program of the offer, including daily itineraries and conditions.")
    included_services: List[str] = Field(..., description="A list of services included in the price, typically found under 'Цената включва'.")
    excluded_services: List[str] = Field(..., description="A list of services not included in the price, typically found under 'Цената не включва'.")
