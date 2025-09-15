"""
Pydantic models and data processing utilities for Yelp business data.
"""

from datetime import datetime
from typing import Dict, Any, Optional, Union, List, TypeVar, cast

from pydantic import BaseModel, Field, field_validator, model_validator


class YelpBusiness(BaseModel):
    """
    Pydantic model representing a Yelp business with extracted data.
    """
    business_name: str
    years_in_business: Optional[int] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    industry: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    
    @field_validator('rating')
    @classmethod
    def validate_rating(cls, v: Optional[float]) -> Optional[float]:
        """Ensure rating is between 0 and 5 if present."""
        if v is not None:
            if not isinstance(v, (int, float)):
                try:
                    v = float(v)
                except (ValueError, TypeError):
                    return None
            
            v = float(v)
            if v < 0:
                return 0.0
            if v > 5:
                return 5.0
            return round(v * 2) / 2  # Round to nearest 0.5
        return None
    
    @field_validator('review_count')
    @classmethod
    def validate_review_count(cls, v: Optional[int]) -> Optional[int]:
        """Ensure review_count is a non-negative integer if present."""
        if v is not None:
            if not isinstance(v, int):
                try:
                    v = int(float(v))
                except (ValueError, TypeError):
                    return None
            
            if v < 0:
                return 0
            return v
        return None
    
    @field_validator('years_in_business')
    @classmethod
    def validate_years_in_business(cls, v: Optional[int]) -> Optional[int]:
        """Ensure years_in_business is a non-negative integer if present."""
        if v is not None:
            if not isinstance(v, int):
                try:
                    v = int(float(v))
                except (ValueError, TypeError):
                    return None
            
            if v < 0:
                return 0
            return v
        return None
    
    @model_validator(mode='after')
    def validate_model(self) -> 'YelpBusiness':
        """Ensure all fields are properly formatted."""
        # Ensure business_name is not empty
        if not self.business_name or not self.business_name.strip():
            raise ValueError("business_name cannot be empty")
        
        # Clean up phone number if present
        if self.phone:
            self.phone = clean_phone_number(self.phone)
        
        # Clean up email if present
        if self.email:
            self.email = self.email.strip().lower()
        
        # Clean up website if present
        if self.website:
            self.website = self.website.strip()
        
        return self
    
    def dict_for_dataset(self) -> Dict[str, Any]:
        """Convert to a dictionary suitable for the Apify dataset."""
        return self.model_dump(exclude_none=True)


def compute_years_in_business(founding_year: Union[int, str, None]) -> Optional[int]:
    """
    Compute years in business from a founding year.
    
    Args:
        founding_year: The year the business was founded, as int or string.
        
    Returns:
        The number of years the business has been operating, or None if invalid.
    """
    if founding_year is None:
        return None
    
    try:
        # Convert to int if it's a string
        if isinstance(founding_year, str):
            # Extract just the year if it's in a date format
            if '-' in founding_year or '/' in founding_year:
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        founding_year = datetime.strptime(founding_year, fmt).year
                        break
                    except ValueError:
                        continue
            
            # Try to extract just digits if it's mixed with text
            import re
            year_match = re.search(r'\b(19\d{2}|20\d{2})\b', founding_year)
            if year_match:
                founding_year = int(year_match.group(1))
            else:
                # Just try to convert the whole string
                founding_year = int(founding_year)
        
        # Now founding_year should be an int
        founding_year = int(founding_year)
        
        # Validate the year is reasonable
        current_year = datetime.now().year
        if founding_year < 1800 or founding_year > current_year:
            return None
        
        years = current_year - founding_year
        return max(0, years)  # Ensure non-negative
    
    except (ValueError, TypeError):
        return None


def merge_business_data(primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two business data dictionaries, keeping values from primary if present,
    and filling in missing (None) values from secondary.
    
    Args:
        primary: The primary business data dictionary.
        secondary: The secondary business data dictionary to fill gaps.
        
    Returns:
        A merged dictionary with values from primary taking precedence.
    """
    result = primary.copy()
    
    for key, sec_value in secondary.items():
        # Only use secondary value if the key is missing in primary or primary value is None
        if key not in result or result[key] is None:
            result[key] = sec_value
    
    return result


def clean_phone_number(phone: str) -> str:
    """
    Clean and format a phone number string.
    
    Args:
        phone: The raw phone number string.
        
    Returns:
        A cleaned phone number string.
    """
    if not phone:
        return ""
    
    # Remove non-digit characters except + for international prefix
    import re
    digits_only = re.sub(r'[^\d+]', '', phone)
    
    # If it starts with a +, keep it as international format
    if digits_only.startswith('+'):
        return digits_only
    
    # For US numbers, format as (XXX) XXX-XXXX if 10 digits
    if len(digits_only) == 10:
        return f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
    
    # For US numbers with country code, format with +1
    if len(digits_only) == 11 and digits_only.startswith('1'):
        return f"+{digits_only}"
    
    # Otherwise return as is
    return digits_only
