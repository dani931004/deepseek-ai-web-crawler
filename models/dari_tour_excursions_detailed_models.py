# models/dari_tour_excursions_detailed_models.py

from pydantic import BaseModel, Field
from typing import List, Optional

class DariTourExcursionDetailedOffer(BaseModel):
    """
    Represents a detailed excursion offer from Dari Tour website.
    """
    offer_name: str = Field(..., description="The name/title of the detailed excursion offer")
    program: str = Field(..., description="Detailed program description of the excursion")
    included_services: List[str] = Field(default_factory=list, description="List of services included in the price")
    excluded_services: List[str] = Field(default_factory=list, description="List of services not included in the price")
    additional_excursions: Optional[str] = Field(None, description="Information about additional excursions")

    class Config:
        json_schema_extra = {
            "example": {
                "offer_name": "АВСТРАЛИЯ, с големия Бариерен Риф, НОВА ЗЕЛАНДИЯ, СИНГАПУР и БАНКОК",
                "program": "Detailed daily program description...",
                "included_services": [
                    "самолетни билети за всички международни полети",
                    "23 нощувки със закуски, в хотели 3* и 4*"
                ],
                "excluded_services": [
                    "такса за обработка и подаване на документите за австралийска виза",
                    "Медицинска застраховка"
                ],
                "additional_excursions": "Information about optional excursions and their prices."
            }
        }
