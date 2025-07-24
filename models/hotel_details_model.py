from pydantic import BaseModel, Field
from typing import Optional

class HotelDetails(BaseModel):
    google_map_link: Optional[str] = Field(None, description="Google Maps link extracted from the iframe's src attribute.")
    description: Optional[str] = Field(None, description="Description of the hotel from the 'details-box' div.")
    offer_title: Optional[str] = Field(None, description="Title of the offer the hotel belongs to, from the 'under-page-title' div.")
    hotel_name: Optional[str] = Field(None, description="Name of the hotel.")
    hotel_link: Optional[str] = Field(None, description="Link to the hotel details page.")
