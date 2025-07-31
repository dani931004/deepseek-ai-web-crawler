from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class AngelTravelDetailedOffer(BaseModel):
    offer_name: str = Field(..., description="The name of the offer, usually found in the H1 tag.")
    program: str = Field(..., description="The detailed program of the offer, including daily itineraries and conditions.")
    included_services: List[str] = Field(default_factory=list, description="A list of services included in the price, typically found under 'Цената включва'.")
    excluded_services: List[str] = Field(default_factory=list, description="A list of services not included in the price, typically found under 'Цената не включва'.")
    detailed_offer_link: Optional[str] = Field(None, description="The direct link to the detailed offer page.")
    
