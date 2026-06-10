"""The 14-category consumer taxonomy + other. One primary category per event; tags are cross-cutting."""

CATEGORIES = [
    "live_music",
    "club_nightlife",
    "theatre_performance",
    "comedy",
    "art_exhibitions",
    "film_cinema",
    "talks_literature",
    "workshops_classes",
    "markets_fairs",
    "food_drink",
    "festivals",
    "sports_fitness",
    "family_kids",
    "community_causes",
    "other",
]

TAGS = ["free-entry", "open-air", "family-friendly", "queer", "sold-out"]

LABELS = {
    "live_music": "Live Music",
    "club_nightlife": "Club & Nightlife",
    "theatre_performance": "Theatre & Performance",
    "comedy": "Comedy",
    "art_exhibitions": "Art & Exhibitions",
    "film_cinema": "Film & Cinema",
    "talks_literature": "Talks & Literature",
    "workshops_classes": "Workshops & Classes",
    "markets_fairs": "Markets & Fairs",
    "food_drink": "Food & Drink",
    "festivals": "Festivals",
    "sports_fitness": "Sports & Fitness",
    "family_kids": "Family & Kids",
    "community_causes": "Community & Causes",
    "other": "Other",
}

# schema.org @type → category. DanceEvent is resolved by venue prior in rules.py.
SCHEMA_TYPE_MAP = {
    "MusicEvent": "live_music",
    "TheaterEvent": "theatre_performance",
    "PerformingArtsEvent": "theatre_performance",
    "ComedyEvent": "comedy",
    "ExhibitionEvent": "art_exhibitions",
    "VisualArtsEvent": "art_exhibitions",
    "ScreeningEvent": "film_cinema",
    "LiteraryEvent": "talks_literature",
    "PublicationEvent": "talks_literature",
    "EducationEvent": "workshops_classes",
    "CourseInstance": "workshops_classes",
    "SaleEvent": "markets_fairs",
    "FoodEvent": "food_drink",
    "Festival": "festivals",
    "SportsEvent": "sports_fitness",
    "ChildrensEvent": "family_kids",
    "BusinessEvent": "community_causes",
    "ConferenceEvent": "community_causes",
    "Hackathon": "community_causes",
    "SocialEvent": "community_causes",
    # "Event", "DanceEvent", "EventSeries", "DeliveryEvent" fall through to later tiers
}

# Ticketmaster segment → category; Music and Arts & Theatre split on genre.
TM_SEGMENT_MAP = {
    "Sports": "sports_fitness",
    "Film": "film_cinema",
}


def map_ticketmaster(segment: str | None, genre: str | None) -> str | None:
    if not segment:
        return None
    if segment in TM_SEGMENT_MAP:
        return TM_SEGMENT_MAP[segment]
    if segment == "Music":
        if genre and ("dance" in genre.lower() or "electronic" in genre.lower()):
            return "club_nightlife"
        return "live_music"
    if segment == "Arts & Theatre":
        g = (genre or "").lower()
        if "comedy" in g:
            return "comedy"
        if "fine art" in g or "art" == g:
            return "art_exhibitions"
        return "theatre_performance"
    return None
